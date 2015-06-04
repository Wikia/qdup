package Qdup::Common;

use DBI;

# Get a database handle
sub db {
    my $dbh = DBI->connect( 'DBI:mysql:database=qdup;host=dw-s5', # dsn
                            'qdup', # username
                            '',     # password
                            { AutoCommit => 1,
                              PrintError => 0,
                              RaiseError => 1,
                              mysql_auto_reconnect => 1 } );
    $dbh->do('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED');
    $dbh->do('SET AUTOCOMMIT = 1');
    $dbh->{'mysql_use_result'} = 1;
    return $dbh;
} 

# Execute some SQL
sub db_do {
    my ($sql, $dbh) = @_;
    if (ref($sql) ne 'ARRAY') {
        $sql = [ $sql ];
    }
    my $disconnect = 0;
    if (! defined $dbh) {
        $dbh = Qdup::Common::db;
        $disconnect = 1;
    }
    my $result;
    foreach my $stmt (@$sql) {
        $result = $dbh->do($stmt);
    }
    $dbh->disconnect if $disconnect;
    return $result;
}

# Execute some SQL, return the last ID
sub db_last_id {
    my ($sql, $dbh) = @_;
    my $disconnect = 0;
    if (! defined $dbh) {
        $dbh = Qdup::Common::db;
        $disconnect = 1;
    }
    my $result = $dbh->do($sql);
    $result = Qdup::Common::db_value('SELECT last_insert_id()', $dbh);
    $dbh->disconnect if $disconnect;
    return $result;
}

# Retrieve a single value
sub db_value {
    my ($sql, $dbh) = @_;
    my $disconnect = 0;
    if (! defined $dbh) {
        $dbh = Qdup::Common::db;
        $disconnect = 1;
    }
    my $result = $dbh->selectall_arrayref($sql)->[0][0];
    $dbh->disconnect if $disconnect;
    return $result;
}

# Retrieve a reference to an array of values
sub db_arrvalue {
    my ($sql, $dbh) = @_;
    my $disconnect = 0;
    if (! defined $dbh) {
        $dbh = Qdup::Common::db;
        $disconnect = 1;
    }
    my $arrayref = $dbh->selectall_arrayref($sql);
    $dbh->disconnect if $disconnect;
    foreach (@$arrayref) { $_ = $_->[0] };
    return $arrayref;
}

# Retrieve a reference to an array of result rows (hashrefs)
sub db_arrayref {
    my ($sql, $dbh) = @_;
    my $disconnect = 0;
    if (! defined $dbh) {
        $dbh = Qdup::Common::db;
        $disconnect = 1;
    }
    my $arrayref = $dbh->selectall_arrayref($sql, { Slice => {} } );
    $dbh->disconnect if $disconnect;
    return $arrayref;
}

1;
