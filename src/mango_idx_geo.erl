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

-module(mango_idx_geo).


-export([
    validate_new/1,
    validate_index_def/1,
    add/2,
    remove/2,
    from_ddoc/1,
    to_json/1,
    columns/1,
    is_usable/2
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_idx.hrl").


validate_new(#idx{}=Idx) ->
    {ok, Def} = do_validate(Idx#idx.def),
    {ok, Idx#idx{def=Def}}.


validate_index_def(Def) ->
    def_to_json(Def).


add(#doc{body={Props0}}=DDoc, Idx) ->
    Geo1 = case proplists:get_value(<<"st_indexes">>, Props0) of
        {Geo0} -> Geo0;
        _ -> []
    end,
    NewGeo = make_geo(Idx),
    Geo2 = lists:keystore(element(1, NewGeo), 1, Geo1, NewGeo),
    Props1 = lists:keystore(<<"st_indexes">>, 1, Props0,
    	{<<"st_indexes">>, {Geo2}}),
    {ok, DDoc#doc{body={Props1}}}.


do_validate({Props}) ->
    {ok, Opts} = mango_opts:validate(Props, opts()),
    {ok, {Opts}};
do_validate(Else) ->
    ?MANGO_ERROR({invalid_index_geo, Else}).


remove(#doc{body={Props0}}=DDoc, Idx) ->
    Geo1 = case proplists:get_value(<<"st_indexes">>, Props0) of
        {Geo0} ->
            Geo0;
        _ ->
            ?MANGO_ERROR({index_not_found, Idx#idx.name})
    end,
    Geo2 = lists:keydelete(Idx#idx.name, 1, Geo1),
    if Geo2 /= Geo1 -> ok; true ->
        ?MANGO_ERROR({index_not_found, Idx#idx.name})
    end,
    Props1 = case Geo2 of
        [] ->
            lists:keydelete(<<"st_indexes">>, 1, Props0);
        _ ->
            lists:keystore(<<"st_indexes">>, 1, Props0, {<<"st_indexes">>, {Geo2}})
    end,
    {ok, DDoc#doc{body={Props1}}}.


from_ddoc({Props}) ->
    case lists:keyfind(<<"st_indexes">>, 1, Props) of
        {<<"st_indexes">>, {Geo}} when is_list(Geo) ->
            lists:flatmap(fun({Name, {GProps}}) ->
                case validate_ddoc(GProps) of
                    invalid_geo ->
                        [];
                    {Def, Opts} ->
                        I = #idx{
                        type = <<"geo">>,
                        name = Name,
                        def = Def,
                        opts = Opts
                        },
                        [I]
                end
            end, Geo);
        _ ->
            []
    end.


to_json(Idx) ->
    {[
        {ddoc, Idx#idx.ddoc},
        {name, Idx#idx.name},
        {type, Idx#idx.type},
        {def, {def_to_json(Idx#idx.def)}}
    ]}.


columns(Idx) ->
    {Props} = Idx#idx.def,
    {<<"fields">>, {Fields}} = lists:keyfind(<<"fields">>, 1, Props),
    [Key || {Key, _} <- Fields].


def_to_json({Props}) ->
    def_to_json(Props);
def_to_json([]) ->
    [];
def_to_json([{fields, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{<<"fields">>, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{Key, Value} | Rest]) ->
    [{Key, Value} | def_to_json(Rest)].

%% TODO ----------------------------------------------------------------
is_usable(_Idx, _Selector) ->
    true.


opts() ->
    [
        {<<"fields">>, [
            {tag, fields},
            {validator, fun mango_opts:validate_sort/1}
        ]}
    ].

%% --------------------------------------------------------------------

make_geo(Idx) ->
    Geo = {[
        {<<"index">>, Idx#idx.def},
        {<<"options">>, {Idx#idx.opts}}
    ]},
    {Idx#idx.name, Geo}.


validate_ddoc(GProps) ->
    try
        Def = proplists:get_value(<<"index">>, GProps),
        validate_index_def(Def),
        {Opts0} = proplists:get_value(<<"options">>, GProps),
        Opts = lists:keydelete(<<"sort">>, 1, Opts0),
        {Def, Opts}
    catch Error:Reason ->
        couch_log:error("Invalid Index Def ~p. Error: ~p, Reason: ~p",
            [GProps, Error, Reason]),
        invalid_geo
    end.