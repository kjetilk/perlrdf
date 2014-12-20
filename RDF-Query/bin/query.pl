#!/usr/bin/perl

use strict;
use warnings;
use lib qw(lib);

use File::Spec;
use URI::file;
use RDF::Query;
use RDF::Query::Util;
binmode( \*STDOUT, ':utf8' );

################################################################################
# Log::Log4perl::init( \q[
# 	log4perl.category.rdf.query.util		= DEBUG, Screen
# 	log4perl.category.rdf.query.plan.thresholdunion		= TRACE, Screen
# 	log4perl.appender.Screen				= Log::Log4perl::Appender::Screen
# 	log4perl.appender.Screen.stderr			= 0
# 	log4perl.appender.Screen.layout			= Log::Log4perl::Layout::SimpleLayout
# ] );
################################################################################

# construct a query from the command line arguments
my $query = RDF::Query->new('PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#> SELECT ?server (count(?server) AS ?count) WHERE { GRAPH ?g { ?site <urn:app:freshtime:hard> ?hard ;  <urn:app:hasrequest> ?request .   ?request <http://www.w3.org/2007/ont/http#hasResponse> ?response .  ?response <http://www.w3.org/2007/ont/httph#server> ?server .  FILTER (xsd:integer(?hard) > 0) }} GROUP BY ?server ORDER BY ?count');

use RDF::Trine;

my $store = RDF::Trine::Store::Memory->new_with_string('Memory;file://'.join(';file://', glob('/mnt/ssdstore/data/btc-processed/run6/crawl/*')));
my $model = RDF::Trine::Model->new($store);



warn "load complete";
unless ($query and $model) {
	die RDF::Query->error;
}

warn "planning";
my ($plan, $context) = $query->prepare ( $model );

warn $plan->explain('  ', 0);

# execute the query against data contained in the model
my $iter	= $query->execute_plan ( $plan, $context );

# print the results as a string to standard output
print $iter->as_string;

print "\n";

#print $plan->explain('  ', 0);

### this will allow the results to be printed in a streaming fashion:
### or, if you want to iterate over each result row:
# while (my $s = $iter->next) {
# 	print $s . "\n";
# }
