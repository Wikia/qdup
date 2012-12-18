package Qdup::Job::Base;

use strict;
use warnings;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    return $self;
}

sub execute {
    my ($self, $job_config) = @_;
}

sub retry_on_failure {
    return 0;
}

sub retry_on_timeout {
    return 1;
}

sub retry_delay {
    return 300;
}

sub timeout_seconds {
    return 300;
}

1;
