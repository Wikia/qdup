package Qdup::Worker;

use strict;
use warnings;

use Capture::Tiny ':all';
use Qdup::Common;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;

    $self->{queue} ||= 'main';
    $self->{sleep} ||= 0;
    $self->{id}    ||= 1;

    $self->{host} = `hostname`;
    chomp($self->{host});

    $self->{worker} = "$self->{host}:$$";  # host:pid

    eval "use lib qw($self->{lib})" if defined $self->{lib};

    open $self->{LOG}, ">> /home/analytics/src/qdup/log/qdup_worker.$self->{queue}.$self->{id}.log";

    return $self;
}

sub run {
    my $self = shift;

    $self->{dbh} ||= Qdup::Common::db;

    if ($self->{pool} && $self->{id}) {
        $self->{pool_filter_sql} = " AND (id % $self->{pool}) = " . ($self->{id}-1);
    } else {
        $self->{pool_filter_sql} = '';
    }

    while (1) {
        sleep($self->{sleep});

        # Get possible jobs
        my $job_batch_sql = 
            "SELECT *
               FROM (SELECT *
                       FROM qdup_jobs
                      WHERE queue  = '$self->{queue}'
                        AND status = 'WAITING'
                        AND worker IS NULL
                        AND (run_after IS NULL OR run_after < now())
                        $self->{pool_filter_sql}
                      ORDER BY priority,
                               FLOOR(id % 1000)
                      LIMIT 2000) sub
              ORDER BY RAND()
              LIMIT 1000";

        $job_batch_sql =~ s/\s+/ /g;

        my $job_batch = [];

        eval {
            $job_batch = Qdup::Common::db_arrayref($job_batch_sql, $self->{dbh});
        };

        if (0 == scalar @$job_batch) {
            $self->log('No jobs found.');
            $self->{sleep} = $self->{sleep} >= 5 ? 5 : $self->{sleep} + 1;
        } else {
            $self->{sleep} = 0;
        }

        JOB:
        foreach my $job_config (@$job_batch) {
            $self->{job_config} = $job_config;

            # Lock job
            my $lock_sql =
                "UPDATE qdup_jobs
                    SET worker = '$self->{worker}',
                        status = 'RUNNING',
                        begin_time = now(),
                        end_time   = null
                  WHERE id = $self->{job_config}->{id}
                    AND queue = '$self->{queue}'
                    AND status = 'WAITING'
                    AND worker IS NULL
                    AND run_after < now()";

            my $lock = 0;

            eval {
                $lock = Qdup::Common::db_do($lock_sql, $self->{dbh});
            };

            if (0 == $lock) {
                # Another worker must have locked and/or completed this job already
                next JOB;
            }

            # Load job class
            if (! defined $INC{ $self->{job_config}->{class} }) {
                eval "use $self->{job_config}->{class}";
                if (my $err = $@) {
                    $self->fail("FAILED to find module '$self->{job_config}->{class}'");
                    next JOB;
                }
            }

            # Instantiate job
            eval {
              $self->{job} = $self->{job_config}->{class}->new();
            };
            if (my $err = $@) {
                $self->fail("FAILED to instantiate job: $err");
                next JOB;
            }

            # Execute job
            eval {
                ($self->{stdout}, $self->{stderr}, $self->{result}) = capture {
                    return scalar $self->{job}->execute($self->{job_config});
                };
            };
            if (my $err = $@) {
                $self->fail("FAILED during execution: $err");
                next JOB;
            }
            $self->complete;
        }
    }
}

sub log {
    my $self = shift;

    $self->{stdout} ||= '';
    $self->{stderr} ||= '';

    $self->{stdout} =~ s/'/''/g;
    $self->{stderr} =~ s/'/''/g;

    chomp($self->{stdout});
    chomp($self->{stderr});

    if ($self->{stdout} ne '' || $self->{stderr} ne '') {
        eval {
            Qdup::Common::db_do(
               "REPLACE INTO qdup_job_logs (
                    id,
                    stdout,
                    stderr
                ) VALUES (
                    $self->{job_config}->{id},
                    '$self->{stdout}',
                    '$self->{stderr}'
                )"
            );
        };
        if (my $err = $@) {
            print { $self->{LOG} } $err;
        }
    } else {
        eval {
            Qdup::Common::db_do("DELETE FROM qdup_job_logs WHERE id = $self->{job_config}->{id}");
        };
        if (my $err = $@) {
            print { $self->{LOG} } $err;
        }
    }
    $self->{stdout} = undef;
    $self->{stderr} = undef;
}

sub fail {
    my ($self, $msg) = @_;
    $self->{stderr} .= $msg;
    $self->log;
    eval {
        Qdup::Common::db_do(
            "UPDATE qdup_jobs
                SET worker    = null,
                    status    = 'FAILED',
                    end_time  = now(),
                    run_after = DATE_ADD(now(), INTERVAL 1 MINUTE)
              WHERE id = $self->{job_config}->{id}",
            $self->{dbh}
        );
    };
    if (my $err = $@) {
        print { $self->{LOG} } $err;
    }
    $self->{job}        = undef;
    $self->{job_config} = undef;
}

sub complete {
    my $self = shift;
    $self->log;
    eval {
        Qdup::Common::db_do(
            "UPDATE qdup_jobs
                SET worker   = null,
                    status   = 'COMPLETED',
                    end_time = now()
              WHERE id = $self->{job_config}->{id}",
            $self->{dbh}
        );
    };
    if (my $err = $@) {
        print { $self->{LOG} } $err;
    }
    $self->{job}        = undef;
    $self->{job_config} = undef;
}

sub DESTROY {
    my $self = shift;
    close $self->{LOG} if $self->{LOG};
}

1;
