package Helios::Service::SolrIndexer;

use strict;
use warnings;
use base qw(Helios::Service);
use Error qw(:try);
use Sys::Syslog qw(:macros);

use HTTP::Request;
use LWP::UserAgent;
use Encode;
use XML::Writer;

our $VERSION = '0.01_01';

=head1 NAME

Helios::Service::SolrIndexer - a demostration indexing application for the Helios job processing 
framework

=head1 DESCRIPTION

Helios::Service::SolrIndexer (SolrIndexer for short) is a simple example application to 
demonstrate the typical Helios application design pattern in the context of a search engine index 
update (in this case, Apache Solr). 
=head1 HELIOS CONFIG PARAMETERS

SolrIndexer does require several config parameters to be defined in your Helios collective for it 
to function correctly.  These can be placed in either helios.ini or the Helios Ctrl Panel (the 
Ctrl Panel method is recommended):

=over 4

=item index_endpoint

The URI endpoint of the Solr index (eg http://localhost:8983/solr)

=item source_dsn 	

The DBI datasource name of the database table to be indexed

=item source_user 	

Username to use to connect to the source database.

=item source_password

Password to use to connect the source database.

=item source_tb 	

Name of the table to be indexed in the source database.

=item source_fields

A comma-delimited string specifying which of the source table's fields should be selected and given
to Solr to index.  Remember, these must be set up in the Solr index's schema beforehand, or Solr 
will just return an error when an update is attempted.

=item source_id_field 	

The field name of the primary key in the source field in the database.  The values of this field 
will passed in via the job arguments and a SQL WHERE clause built around it to uniquely identify 
the record in the database table.  The contents of this field will also become the id of the 
document in the Solr index.

=back

=head1 JOB ARGUMENTS

Job arguments for this service should be specified in the form:

 <params>
   <id>1234</id>
 </params>

where the <id> section contains the primary key of the source table to be indexed in the database.

=head1 METHODS

=head2 run($helios_job)

As is typical for Helios services, run() is the main workhorse of SolrIndexer.  It will be called 
by Helios workers to service a job.  The $helios_job passed to it will be a Helios::Job object.

Once run() has pulled in its configuration hashref and parsed the Helios::Job object's argument 
XML, run() performs 4 tasks to accomplish a job:

=over 4

=item 1

Generates the SQL to retrieve the records from the database

=item 2

Executes the SQL with the id given to it in the job arguments

=item 3

Reformats the retrieved database record into a UTF-8 encoded XML stream for Solr (Solr requires 
UTF-8 encoding)

=item 4

Sends the XML stream to Solr to be added to the index

=back

If all these steps are successful, run() calls Helios::Service->completedJob() to mark the job as 
completed successfully.  If an error occurs, it calls Helios::Service->logMsg() to log the error 
message and Helios::Service->failedJob() to mark the job as failed.

=cut

sub run {
	my $self = shift;
    my $job = shift;
    my $config = $self->getConfig();
    my $args = $self->getJobArgs($job);

    try {
        my $id = $args->{id};
        $self->logMsg($job, LOG_INFO, 'Adding '.$config->{source_id_field}.' '.$args->{id}.' to the index');
        my $sql = $self->generateSQL();
        if ($self->debug) { print "SQL: $sql\n"; }

        my $dbresult = $self->retrieveFromDb($sql, $id);
  
        my $xml = $self->generateXML($dbresult);
        if ($self->debug) { print "XML: $xml\n"; }
        
        $self->updateIndex($xml);

        $self->logMsg($job, LOG_INFO, $config->{source_id_field}.' '.$args->{id}.' successfully added to the index');
        $self->completedJob($job);
    } catch Helios::Error::Fatal with {
        my $e = shift;
        $self->logMsg($job, LOG_ERR, "Error: ".$e->text);
        $self->failedJob($job, $e->text);
    } catch Helios::Error::FatalNoRetry with {
        my $e = shift;
        $self->logMsg($job, LOG_ERR, "Error (permanent): ".$e->text);
        $self->failedJob($job, $e->text);
    } otherwise {
        my $e = shift;
        $self->logMsg($job, LOG_ERR, "Unexpected error: ".$e->text);
        $self->failedJob($job, $e->text);
    };
}


=head2 generateSQL()

Generates the SQL necessary to retrieve the database record.  This method determines the correct 
SQL by looking at the configuration parameters defined in Helios.

=cut

sub generateSQL {
    my $self = shift;
    my $config = $self->getConfig();
    my $table = $config->{source_tb};
    my $fields = $config->{source_fields};
    my $id_field = $config->{source_id_field};
    return "SELECT $fields FROM $table WHERE $id_field = ?";
}


=head2 retrieveFromDb($sql, $id)

Given a SQL SELECT statement and a unique id, retrieveFromDb() retrieves the record identified by 
the $id using the supplied $sql.  It returns the record in the form of a hashref.

=cut

sub retrieveFromDb {
    my $self = shift;
    my $sql = shift;
    my $id = shift;
    my $config = $self->getConfig();
   
    my $dbh = $self->dbConnect($config->{source_dsn}, $config->{source_user}, $config->{source_password});
    my $sth = $dbh->prepare($sql);
    $sth->execute($id);
    my $result = $sth->fetchrow_hashref();
    $sth->finish();
    return $result;
}


=head2 generateXML($hashref)

Given a hashref, generateXML() takes the hashref's keys and values and turns them into an XML 
stream be passed to Solr.  It returns this string of XML to the calling routine.

=cut

sub generateXML {
    my $self = shift;
    my $result = shift;
    my $xml;

    my $wtr = new XML::Writer(OUTPUT => \$xml, ENCODING => 'utf-8');
    $wtr->startTag('add');
        $wtr->startTag('doc');
            foreach (keys %$result) {
                $wtr->startTag("field", "name" => $_);
                my $text = encode("utf-8", $result->{$_});
                $wtr->characters($text);
                $wtr->endTag("field");
            }
        $wtr->endTag('doc');
    $wtr->endTag('add');
    $wtr->end();
    return $xml;
}


=head2 updateIndex($xml)

Given a Solr XML document addition stream, updateIndex() builds an HTTP::Request object using the 
stream and the Solr endpoint URI.  It then uses LWP::UserAgent to POST the request to Solr.  If 
the document update is successful, updateIndex() returns the successful status (usually '200 OK' to
the calling routine.  If the request was not successful, the method will throw a 
Helios::Error::Fatal exception with the erroneous status as the message.

=cut

sub updateIndex {
    my $self = shift;
    my $xml = shift;
    my $config = $self->getConfig();
    my $endpoint = $config->{index_endpoint};
    
    # generate URL
#    if ($endpoint =~ /\/$/) { chop $endpoint; }     # solr doesn't like //
    my $url = $endpoint . '/update';
    
    # put together the request
    my $request = HTTP::Request->new(POST => $url);
    $request->header('Content-type' => 'text/xml', 'charset' => 'utf-8');
    $request->content($xml);
    
    my $ua = LWP::UserAgent->new();
    my $response = $ua->request($request);
    unless ($response->is_success) {
        throw Helios::Error::Fatal($response->status_line);
    }
    return $response->status_line();
}




1;
__END__


=head1 SEE ALSO

L<Helios>, L<LWP::UserAgent>, L<HTTP::Request>, <XML::Writer>, L<DBI>

=head1 AUTHOR

Andrew Johnson, E<lt>lajandy at cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.0 or,
at your option, any later version of Perl 5 you may have available.

=head1 WARRANTY

This software comes with no warranty of any kind.

=cut

