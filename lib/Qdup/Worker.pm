package Qdup::Worker;

use strict;
use warnings;

use Capture::Tiny ':all';
use Carp;
use FindBin qw/$Bin/;
use Qdup::Common;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;

    $self->{queue}    ||= 'main';
    $self->{table}    ||= 'qdup_jobs';
    $self->{sleep}    ||= 0;
    $self->{id}       ||= 1;
    $self->{work_dir} ||= "$Bin/../log";

    $self->{host} = `hostname`;
    chomp($self->{host});

    $self->{worker} = "$self->{host}:$$";  # host:pid

    open $self->{LOG}, ">> $self->{work_dir}/qdup_worker.$self->{queue}.$self->{id}.log";
    select((select($self->{LOG}), $|=1)[0]);

    if (defined $self->{lib}) {
        eval "use lib qw($self->{lib})";
        if (my $err = $@) {
            $self->log("FAILED to include lib $self->{lib}") && confess "FAILED to include lib $self->{lib}";
        }
    }

    $self->{pool_filter_sql} = " AND (id % $self->{pool}) = " . ($self->{id}-1);

    return $self;
}

sub done {
    my ($self, $status, $msg) = @_;
    $msg ||= '';
    $self->log("$status [$self->{job_config}->{id}] $msg");
    my $result = 0;
    eval {
        my $retry = 0;
        my $retry_delay = 0;

        if ($status eq 'FAILED') {
            $retry = $self->{job}->retry_on_failure;
        } elsif ($status eq 'TIMEDOUT') {
            $retry = $self->{job}->retry_on_timeout;
        }

        if ($retry) {
            $retry_delay = $self->{job}->retry_delay;
        }

        $result = Qdup::Common::db_do(
            "UPDATE $self->{table}
                SET worker    = '',
                    status    = CASE WHEN '$retry' = 1 THEN 'WAITING'
                                     ELSE '$status' END,
                    end_time  = CASE WHEN '$retry' = 1 THEN null
                                     ELSE now() END,
                    run_after = CASE WHEN '$status' = 'COMPLETED' OR '$retry' = 0 THEN run_after
                                     ELSE DATE_ADD(now(), INTERVAL $retry_delay SECOND) END
              WHERE id = $self->{job_config}->{id}"
        );
    };
    if (my $err = $@) {
        $self->log("ERROR marking $self->{table} row after done [$self->{job_config}->{id}]: $err");
    }
    if (1 != $result) {
        $self->log("ERROR no rows marked as done [$self->{job_config}->{id}]");
    }
}

sub log {
    my ($self, $msg) = @_;
    chomp($msg);
    my $ts = `date +"%Y-%m-%d %H:%M:%S"`;
    chomp($ts);
    print { $self->{LOG} } "$ts : $msg\n";
}

sub run {
    my $self = shift;

    while (1) {
        sleep($self->{sleep});

        # Get possible jobs
        my $job_batch_sql = 
            "SELECT *
               FROM (SELECT *
                       FROM $self->{table}
                      WHERE queue  = '$self->{queue}'
                        AND status = 'WAITING'
                        AND worker = ''
                        AND (run_after IS NULL OR run_after < now())
                        $self->{pool_filter_sql}
                      ORDER BY priority,
                               id
                      LIMIT 1000) sub
              ORDER BY priority,
                       RAND()
              LIMIT 250";

        my $job_batch = [];

        eval {
            $job_batch = Qdup::Common::db_arrayref($job_batch_sql);
        };

        if (0 == scalar @$job_batch) {
            # No available jobs; remove the filter and sleep a little longer
            $self->{pool_filter_sql} = '';
            $self->{sleep} = $self->{sleep} >= 30 ? 30 : $self->{sleep} + 5;
        } else {
            $self->{pool_filter_sql} = " AND (id % $self->{pool}) = " . ($self->{id}-1);
            $self->{sleep} = 0;
        }

        JOB:
        foreach my $job_config (@$job_batch) {
            $self->{job_config} = $job_config;

            # Lock job
            my $lock_sql =
                "UPDATE $self->{table}
                    SET worker = '$self->{worker}',
                        status = 'RUNNING',
                        begin_time = now(),
                        end_time   = null
                  WHERE id = $self->{job_config}->{id}
                    AND queue  = '$self->{queue}'
                    AND status = 'WAITING'
                    AND worker = ''
                    AND run_after < now()";

            my $lock = 0;

            eval {
                $lock = Qdup::Common::db_do($lock_sql);
            };

            if (0 == $lock) {
                # Another worker must have locked and/or completed this job already
                next JOB;
            }

            $self->log("WORKING [$self->{job_config}->{id}]");

            # TODO: force unload of class module (on demand?)

            # Load job class
            if (! defined $INC{ $self->{job_config}->{class} }) {
                eval "use $self->{job_config}->{class}";
                if (my $err = $@) {
                    $self->done('FAILED', "LOAD ERROR: $err");
                    next JOB;
                }
            }

            # Instantiate job
            eval {
              $self->{job} = $self->{job_config}->{class}->new();
            };
            if (my $err = $@) {
                $self->done('FAILED', "INSTANTIATION ERROR: $err");
                next JOB;
            }

            # Clear output variables
            $self->{stdout} = '';
            $self->{stderr} = '';
            $self->{result} = '';

            # Execute job
            eval {
                ($self->{stdout}, $self->{stderr}, $self->{result}) = capture {
                    local $SIG{ALRM} = sub { confess 'TIMEDOUT' };
                    alarm $self->{job}->timeout_seconds;
                    my $rv = scalar $self->{job}->execute($self->{job_config});
                    alarm 0;
                    return $rv;
                };
            };
            if (my $err = $@) {
                alarm 0;
                if ($err =~ m/TIMEDOUT/) {
                    $self->done('TIMEDOUT', "TIMEOUT ERROR: $err");
                } else {
                    $self->done('FAILED', "ERROR: $err ; stderr: $self->{stderr}");
                }
                next JOB;
            }
            $self->done('COMPLETED');

        }  # End of batch of jobs
    }  # End of loop for retrieving a batch of jobs
}

sub DESTROY {
    my $self = shift;
    close $self->{LOG} if $self->{LOG};
}

1;
