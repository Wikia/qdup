package Qdup::Worker;

use strict;
use warnings;

use Capture::Tiny ':all';
use Carp;
use FindBin qw/$Bin/;
use Qdup::Common;

our $STATUS_ON_HOLD   = 'ON HOLD';
our $STATUS_WAITING   = 'WAITING';
our $STATUS_RUNNING   = 'RUNNING';
our $STATUS_TIMEDOUT  = 'TIMEDOUT';
our $STATUS_FAILED    = 'FAILED';
our $STATUS_COMPLETED = 'COMPLETED';

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;

    $self->{queue}    ||= 'main';
    $self->{table}    ||= 'qdup_jobs';
    $self->{sleep}    ||= 0;
    $self->{orig_sleep} = $self->{sleep};
    $self->{id}       ||= 1;
    $self->{work_dir} ||= "$Bin/../log";

    $self->{host} = `hostname`;
    chomp($self->{host});

    $self->{worker} = "$self->{host}:$$";  # host:pid

    open $self->{LOG}, ">> $self->{work_dir}/qdup_worker.$self->{queue}.$self->{id}.log";
    select((select($self->{LOG}), $|=1)[0]);  # disable log buffering

    eval "use lib qw($self->{lib})" if (defined $self->{lib});

    $self->{pool_filter_sql} = " AND (id % $self->{pool}) = " . ($self->{id}-1);
    $self->{orig_pool_filter_sql} = $self->{pool_filter_sql};

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

        if ($status eq $STATUS_FAILED) {
            $retry = $self->{job}->retry_on_failure;
        } elsif ($status eq $STATUS_TIMEDOUT) {
            $retry = $self->{job}->retry_on_timeout;
        }

        if ($retry) {
            $retry_delay = $self->{job}->retry_delay;
        }

        $result = Qdup::Common::db_do(
            "UPDATE $self->{table}
                SET worker    = '',
                    status    = CASE WHEN '$retry' = 1 THEN '$STATUS_WAITING'
                                     ELSE '$status' END,
                    end_time  = CASE WHEN '$retry' = 1 THEN null
                                     ELSE now() END,
                    run_after = CASE WHEN '$status' = '$STATUS_COMPLETED' OR '$retry' = 0 THEN run_after
                                     ELSE DATE_ADD(now(), INTERVAL $retry_delay SECOND) END
              WHERE id = $self->{job_config}->{id}"
        );
    };
    if (my $err = $@) {
        $self->log("ERROR marking $self->{table} row after done [$self->{job_config}->{id}]: $err");
    }
    if (1 != $result) {
        $self->log("ERROR no rows marked as done [$self->{job_config}->{id}]");
    } else {
        if ($self->{stdout}) {
            $self->log("STDOUT [$self->{job_config}->{id}]");
            $self->log($self->{stdout});
        }
        if ($self->{stderr}) {
            $self->log("STDERR [$self->{job_config}->{id}]");
            $self->log($self->{stderr});
        }
    }
}

sub log {
    my ($self, $msg) = @_;
    chomp($msg);
    my $ts = `date +"%Y-%m-%d %H:%M:%S"`;
    chomp($ts);
    $msg =~ s/(^|\n)/$1$ts /g;
    print { $self->{LOG} } "$msg\n";
}

sub run {
    my $self = shift;

    while (1) {
        # Get the next 1000 jobs by priority and insertion order,
        #   then randomize and filter to 250 to minimize possible collisions with other workers
        my $job_batch_sql = 
            "SELECT *
               FROM (SELECT *
                       FROM $self->{table}
                      WHERE queue  = '$self->{queue}'
                        AND status = '$STATUS_WAITING'
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
            # No available jobs; remove the filter (to query for other worker's WAITING jobs) and sleep a little longer
            $self->{pool_filter_sql} = '';
            $self->{sleep} = $self->{sleep} >= 30 ? 30 : $self->{sleep} + 5;
        } else {
            # Found jobs, make sure next lookup uses original filter and sleep values
            $self->{pool_filter_sql} = $self->{orig_pool_filter_sql};
            $self->{sleep}           = $self->{orig_sleep};
        }

        JOB:
        foreach my $job_config (@$job_batch) {
            $self->{job_config} = $job_config;

            # Lock job
            my $lock_sql =
                "UPDATE $self->{table}
                    SET worker = '$self->{worker}',
                        status = '$STATUS_RUNNING',
                        begin_time = now(),
                        end_time   = null
                  WHERE id = $self->{job_config}->{id}
                    AND queue  = '$self->{queue}'
                    AND status = '$STATUS_WAITING'
                    AND worker = ''
                    AND (run_after IS NULL OR run_after < now())";

            my $lock = 0;

            eval {
                $lock = Qdup::Common::db_do($lock_sql);
            };

            if (0 == $lock) {
                # Another worker must have locked and/or completed this job already
                next JOB;
            }

            $self->log("RUNNING [$self->{job_config}->{id}]");

            # TODO: force unload of class module (on demand?)

            # Load job class
            if (! defined $INC{ $self->{job_config}->{class} }) {
                eval "use $self->{job_config}->{class}";
                if (my $err = $@) {
                    $self->done($STATUS_FAILED, $err);
                    next JOB;
                }
            }

            # Instantiate job
            eval {
              $self->{job} = $self->{job_config}->{class}->new();
            };
            if (my $err = $@) {
                $self->done($STATUS_FAILED, $err);
                next JOB;
            }

            # Clear output variables
            $self->{stdout} = '';
            $self->{stderr} = '';
            $self->{result} = '';

            # Execute job
            eval {
                ($self->{stdout}, $self->{stderr}, $self->{result}) = capture {
                    local $SIG{ALRM} = sub { confess $STATUS_TIMEDOUT };
                    alarm $self->{job}->timeout_seconds;
                    my $rv = scalar $self->{job}->execute($self->{job_config});
                    alarm 0;
                    return $rv;
                };
            };
            if (my $err = $@) {
                alarm 0;  # turn off the alarm!
                if ($err =~ m/$STATUS_TIMEDOUT/) {
                    $self->done($STATUS_TIMEDOUT, $err);
                } else {
                    $self->done($STATUS_FAILED, $err);
                }
                next JOB;
            }
            $self->done($STATUS_COMPLETED);

        }  # End of batch of jobs

        sleep($self->{sleep});

    }  # End of loop for retrieving a batch of jobs
}

sub DESTROY {
    my $self = shift;
    close $self->{LOG} if $self->{LOG};
}

1;
