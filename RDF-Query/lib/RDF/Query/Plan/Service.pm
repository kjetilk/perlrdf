# RDF::Query::Plan::Service
# -----------------------------------------------------------------------------

=head1 NAME

RDF::Query::Plan::Service - Executable query plan for remote SPARQL queries.

=head1 METHODS

=over 4

=cut

package RDF::Query::Plan::Service;

use strict;
use warnings;
use base qw(RDF::Query::Plan);

use Data::Dumper;
use Scalar::Util qw(blessed);
use Storable qw(store_fd fd_retrieve);
use URI::Escape;

use RDF::Query::ExecutionContext;
use RDF::Query::VariableBindings;

=item C<< new ( $endpoint, $sparql ) >>

=cut

sub new {
	my $class	= shift;
	my $url		= shift;
	my $sparql	= shift;
	my $self	= $class->SUPER::new( $url, $sparql );
	if (@_) {
		# extra args (like the bound/free stuff for logging
		my %args	= @_;
		@{ $self->[0] }{ keys %args }	= values %args;
	}
	return $self;
}

=item C<< execute ( $execution_context ) >>

=cut

sub execute ($) {
	my $self	= shift;
	my $context	= shift;
	if ($self->state == $self->OPEN) {
		throw RDF::Query::Error::ExecutionError -text => "SERVICE plan can't be executed while already open";
	}
	my $endpoint	= $self->[1];
	my $sparql		= $self->[2];
	my $url			= $endpoint . '?query=' . uri_escape($sparql);
	my $query	= $context->query;
	
	my $pid = open my $fh, "-|";
	die unless defined $pid;
	unless ($pid) {
		$RDF::Trine::Store::DBI::IGNORE_CLEANUP	= 1;
		$self->_get_and_parse_url( $context, $url, $fh, $pid );
		exit 0;
	}
	
	my $count	= 0;
	my $open	= 1;
	my $args	= fd_retrieve $fh or die "I can't read args from file descriptor\n";
	if (ref($args)) {
		$self->[0]{args}	= $args;
		$self->[0]{fh}		= $fh;
		$self->[0]{'open'}	= 1;
		$self->[0]{'count'}	= 0;
		$self->state( $self->OPEN );
	} else {
		warn "no iterator in execute()";
	}
}

=item C<< next >>

=cut

sub next {
	my $self	= shift;
	unless ($self->state == $self->OPEN) {
		throw RDF::Query::Error::ExecutionError -text => "next() cannot be called on an un-open SERVICE";
	}
	return undef unless ($self->[0]{'open'});
	my $fh	= $self->[0]{fh};
	my $result = fd_retrieve $fh or die "I can't read from file descriptor\n";
	if (not($result) or ref($result) ne 'HASH') {
		if (my $log = $self->[0]{logger}) {
			$log->push_key_value( 'cardinality-service', $self->[2], $self->[0]{'count'} );
			if (my $bf = $self->[0]{ 'log-service-pattern' }) {
				$log->push_key_value( 'cardinality-bf-service-' . $self->[1], $bf, $self->[0]{'count'} );
			}
		}
		$self->[0]{'open'}	= 0;
		return undef;
	}
	$self->[0]{'count'}++;
	my $row	= RDF::Query::VariableBindings->new( $result );
	return $row;
};

=item C<< close >>

=cut

sub close {
	my $self	= shift;
	unless ($self->state == $self->OPEN) {
		throw RDF::Query::Error::ExecutionError -text => "close() cannot be called on an un-open SERVICE";
	}
	delete $self->[0]{args};
	delete $self->[0]{fh};
	delete $self->[0]{'open'};
	delete $self->[0]{count};
	$self->SUPER::close();
}

sub _get_and_parse_url {
	my $self	= shift;
	my $context	= shift;
	my $url		= shift;
	my $fh		= shift;
	my $pid		= shift;
	my $query	= $context->query;

	eval "
		require XML::SAX::Expat;
		require XML::SAX::Expat::Incremental;
	";
	if ($@) {
		die $@;
	}
	local($XML::SAX::ParserPackage)	= 'XML::SAX::Expat::Incremental';
	my $handler	= RDF::Trine::Iterator::SAXHandler->new();
	my $p	= XML::SAX::Expat::Incremental->new( Handler => $handler );
	$p->parse_start;
	
	my $has_head	= 0;
	my $callback	= sub {
		my $content	= shift;
		my $resp	= shift;
		my $proto	= shift;
		unless ($resp->is_success) {
			throw RDF::Query::Error -text => "SERVICE query couldn't get remote content: " . $resp->status_line;
		}
		$p->parse_more( $content );
		
		if (not($has_head) and $handler->has_head) {
			my @args	= $handler->iterator_args;
			if (exists( $args[2]{Handler} )) {
				delete $args[2]{Handler};
			}
			$has_head	= 1;
			store_fd \@args, \*STDOUT or die "PID $pid can't store!\n";
		}
		
		while (my $data = $handler->pull_result) {
			store_fd $data, \*STDOUT or die "PID $pid can't store!\n";
		}
	};
	my $ua			= ($query)
					? $query->useragent
					: do {
						my $u = LWP::UserAgent->new( agent => "RDF::Query/${RDF::Query::VERSION}" );
						$u->default_headers->push_header( 'Accept' => "application/sparql-results+xml;q=0.9,application/rdf+xml;q=0.5,text/turtle;q=0.7,text/xml" );
						$u;
					};

	$ua->get( $url, ':content_cb' => $callback );
	store_fd \undef, \*STDOUT;
}

=item C<< pattern >>

Returns the query plan that will be sent to the remote service.

=cut

sub pattern {
	my $self	= shift;
	return $self->[2];
}


1;

__END__

=back

=head1 AUTHOR

 Gregory Todd Williams <gwilliams@cpan.org>

=cut
