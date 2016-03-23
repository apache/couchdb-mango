% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mango_selector_geo).


-export([
    make_geom/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").


make_geom(Selector) ->
    % We extract the geospatial portion of the selector so that we can
    % convert it into a geometry object for hastings. The rest of the
    % selector will then be used to filter the returned docs.
    twig:log(notice, "Selector ~p", [Selector]),
    {GeomSelector, FilterSelector} = extract_geo_selector(Selector),
    twig:log(notice, "GeomSelector ~p", [GeomSelector]),
    Geom = convert(GeomSelector),
    {Geom, FilterSelector}.


extract_geo_selector({[{<<"$and">>, Args}]}) ->
    Pred = fun ({[{_Field, {[{Op, _}]}}]}) ->
        lists:member(Op, geo_query_operators())
    end,
    {GeomOp, FilterOps} = lists:partition(Pred, Args),
    % need to add in a check here to make sure there's only one geo
    % query operator
    GeomSelector = lists:nth(1, GeomOp),
    case length(FilterOps) of
        Len when Len > 2 ->
            {GeomSelector, {[{<<"$and">>, FilterOps}]}};
        Len when Len =:= 1 ->
            % no need for an $and when only one operator left
            {GeomSelector, lists:nth(1, FilterOps)}
    end;

extract_geo_selector({[{<<"$geoWithin">>, Args}]}) ->
    {[{<<"$geoWithin">>, Args}]};

extract_geo_selector({[{<<"$geoIntersect">>, Args}]}) ->
    {[{<<"$geoIntersect">>, Args}]};

extract_geo_selector({[{<<"$geoNear">>, Args}]}) ->
    {[{<<"$geoNear">>, Args}]};

extract_geo_selector({[{<<"$geoContains">>, Args}]}) ->
    {[{<<"$geoContains">>, Args}]};

extract_geo_selector({[{Field, Cond}]}) ->
    {{[{Field, extract_geo_selector(Cond)}]}, {[]}}.

geo_query_operators() ->
    [<<"$geoWithin">>, <<"$geoIntersect">>, <<"$geoContains">>,
        <<"$geoNear">>].


convert({[{<<"$geoWithin">>, {[{<<"$bbox">>, Coords}]}}]}) ->
    {bbox, Coords};

convert({[{Field, Cond}]}) ->
    convert(Cond).
