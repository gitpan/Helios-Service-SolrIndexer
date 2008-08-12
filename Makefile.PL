use 5.008008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Helios::Service::SolrIndexer',
    VERSION_FROM      => 'lib/Helios/Service/SolrIndexer.pm', # finds $VERSION
    PREREQ_PM         => { Helios => 2.00,
                           Bundle::LWP => '',
                           XML::Writer => '',
                         }, # e.g., Module::Name => 1.1
    ABSTRACT          => 'A demostration indexing application for the Helios job processing framework',
    AUTHOR            => 'Andrew Johnson <lajandy@cpan.org>',
    LICENSE           => 'perl'
);
