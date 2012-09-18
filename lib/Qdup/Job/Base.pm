package Qdup::Job::Base;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    return $self;
}

sub execute {
    my ($self, $job_config) = @_;
}

1;
