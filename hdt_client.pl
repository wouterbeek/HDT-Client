:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(http/http_open)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(uri)).
:- use_module(library(yall)).

:- rdf_meta
   lod(+, r, r, o).

:- thread_local
   lod_cache/1.

%! lod(+Endpoint, ?S, ?P, ?O) is nondet.

lod(Endpoint, S, P, O) :-
  lod_request_uri(Endpoint, S, P, O, Uri),
  lod_request(Uri, S, P, O).

lod_request_uri(Endpoint, S, P, O, Uri) :-
  uri_components(Endpoint, uri_components(Scheme,Authority,Path,_,_)),
  maplist(lod_parameter, [s,p,o], [S,P,O], [SParam,PParam,OParam]),
  include(ground, [SParam,PParam,OParam], QueryComps),
  uri_query_components(Query, [page_size(100)|QueryComps]),
  uri_components(Uri, uri_components(Scheme,Authority,Path,Query,_)).

lod_parameter(_, Var, _) :-
  var(Var), !.
lod_parameter(Key, Literal, Key=Value) :-
  rdf_is_literal(Literal), !,
  (   Literal = literal(ltag(LTag,Lex))
  ->  format(atom(Value), '"~a"@~a', [Lex,LTag])
  ;   Literal = literal(type(D,Lex))
  ->  format(atom(Value), '"~a"^^<~a>', [Lex,D])
  ).
lod_parameter(Key, Iri, Key=Value) :-
  rdf_is_iri(Iri), !,
  format(atom(Value), '<~a>', [Iri]).
lod_parameter(Key, BNode, Key=BNode) :-
  rdf_is_bnode(BNode), !.
lod_parameter(_, Term, _) :-
  type_error(rdf_term, Term).


lod_request(Uri, S, P, O) :-
  lod_http_open(Uri, In, NextUri),
  call_cleanup(
    rdf_process_ntriples(In, lod_assert, []),
    close(In)
  ),
  (   lod_cache(rdf(S,P,O))
  ;   atom(NextUri),
      lod_request(NextUri, S, P, O)
  ).

lod_http_open(Uri, In, NextUri) :-
  http_open(Uri, In, [header(link, Link),
                      request_header('Accept'='application/n-triples'),
                      status_code(Status)]),
  http_check_status(Status),
  ignore(http_next_uri(Link, NextUri)).

http_check_status(200) :- !.
http_check_status(400) :- !, fail.
http_check_status(Status) :-
  domain_error(oneof([200,400]), Status).

http_next_uri(Link, NextUri) :-
  split_string(Link, ",", " ", Pairs),
  member(Pair, Pairs),
  split_string(Pair, ";", "<> ", [Uri0|Params]),
  member(Param, Params),
  split_string(Param, "=", "\"", ["rel","next"]), !,
  atom_string(NextUri, Uri0).

lod_assert(Triples, _) :-
  maplist([Triple]>>assert(lod_cache(Triple)), Triples).
