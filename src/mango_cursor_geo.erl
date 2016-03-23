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

-module(mango_cursor_geo).

-export([
    create/4,
    %explain/1,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("hastings/src/hastings.hrl").
-include("mango_cursor.hrl").
-include("mango.hrl").


-record(cacc, {
    selector,
    dbname,
    ddocid,
    idx_name,
    h_args,
    bookmark,
    limit,
    skip,
    user_fun,
    user_acc,
    fields
}).


create(Db, Indexes, Selector, Opts0) ->
    Index = case Indexes of
        [Index0] ->
            Index0;
        _ ->
            ?MANGO_ERROR(multiple_geo_indexes)
    end,

    Opts = unpack_bookmark(Opts0),

    %% hardcoded for now
    Limit = 25,
    Skip = couch_util:get_value(skip, Opts, 0),
    Fields = couch_util:get_value(fields, Opts),

    {ok, #cursor{
        db = Db,
        index = Index,
        ranges = null,
        selector = Selector,
        opts = Opts,
        limit = Limit,
        skip = Skip,
        fields = Fields
    }}.

% TODO
%% explain(Cursor) ->

execute(Cursor, UserFun, UserAcc) ->
    #cursor{
        db = Db,
        index = Idx,
        limit = Limit,
        skip = Skip,
        selector = Selector,
        opts = Opts
    } = Cursor,
    {Geom, NewSelector} = mango_selector_geo:make_geom(Selector),
    HArgs = #h_args{
        geom = Geom,
        filter = none
    },
    CAcc = #cacc{
        selector = NewSelector,
        dbname = Db#db.name,
        ddocid = ddocid(Idx),
        idx_name = mango_idx:name(Idx),
        bookmark = get_bookmark(Opts),
        limit = Limit,
        skip = Skip,
        h_args = HArgs,
        user_fun = UserFun,
        user_acc = UserAcc,
        fields = Cursor#cursor.fields
    },
    try
        execute(CAcc)
    catch
        throw:{stop, FinalCAcc} ->
            #cacc{
                bookmark = FinalBM,
                user_fun = UserFun,
                user_acc = LastUserAcc
            } = FinalCAcc,
            JsonBM = pack_bookmark(FinalBM),
            Arg = {add_key, bookmark, JsonBM},
            {_Go, FinalUserAcc} = UserFun(Arg, LastUserAcc),
            {ok, FinalUserAcc}
    end.


execute(CAcc) ->
    case hastings_query(CAcc) of
        {ok, Bookmark, []} ->
            % If we don't have any results from the
            % query it means the request has paged through
            % all possible results and the request is over.
            NewCAcc = CAcc#cacc{bookmark = Bookmark},
            throw({stop, NewCAcc});
        {ok, Bookmark, Hits} ->
            NewCAcc = CAcc#cacc{bookmark = Bookmark},
            HitWithDocs = add_docs(CAcc#cacc.dbname, Hits),
            {ok, FinalCAcc} = handle_hits(NewCAcc, HitWithDocs),
            execute(FinalCAcc)
    end.


hastings_query(CAcc) ->
    #cacc{
        dbname = DbName,
        ddocid = DDocId,
        idx_name = IdxName
    } = CAcc,
    HQArgs = update_hastings_args(CAcc),
    twig:log(notice, "HQArgs in mango~p", [HQArgs]),
    case hastings_fabric_search:go(DbName, DDocId, IdxName, HQArgs) of
        {ok, Hits} ->
            twig:log(notice, "Hits Returned ~p",[Hits]),
            %% Unlike dreyfus, we have manually update the bookmark
            %% for the next iteration
            OldBookmark = HQArgs#h_args.bookmark,
            NewBookmark = hastings_bookmark:update(OldBookmark, Hits),
            {ok, NewBookmark, Hits};
        {error, Reason} ->
            ?MANGO_ERROR({hastings_query_error, {error, Reason}})
    end.


handle_hits(CAcc, []) ->
    {ok, CAcc};

handle_hits(CAcc0, [Hit | Rest]) ->
    CAcc1 = handle_hit(CAcc0, Hit),
    handle_hits(CAcc1, Rest).


handle_hit(CAcc0, Hit) ->
    #cacc{
        limit = Limit,
        skip = Skip
    } = CAcc0,
    CAcc1 = update_bookmark(CAcc0, [Hit]),
    Doc = Hit#h_hit.doc,
    twig:log(notice, "Document Returned ~p", [Doc]),
    case mango_selector:match(CAcc1#cacc.selector, Doc) of
        true when Skip > 0 ->
            CAcc1#cacc{skip = Skip - 1};
        true when Limit == 0 ->
            % We hit this case if the user spcified with a
            % zero limit. Notice that in this case we need
            % to return the bookmark from before this match
            throw({stop, CAcc0});
        true when Limit == 1 ->
            NewCAcc = apply_user_fun(CAcc1, Doc),
            throw({stop, NewCAcc});
        true when Limit > 1 ->
            NewCAcc = apply_user_fun(CAcc1, Doc),
            NewCAcc#cacc{limit = Limit - 1};
        false ->
            CAcc1
    end.


apply_user_fun(CAcc, Doc) ->
    %% Commenting out this for now because we can't separate
    %% geo fields with normal fileds. So we can't really have a subset
    %% of fields to present to the user.
    %% FinalDoc = mango_fields:extract(Doc, CAcc#cacc.fields),
    #cacc{
        user_fun = UserFun,
        user_acc = UserAcc
    } = CAcc,
    case UserFun({row, Doc}, UserAcc) of
        {ok, NewUserAcc} ->
            CAcc#cacc{user_acc = NewUserAcc};
        {stop, NewUserAcc} ->
            throw({stop, CAcc#cacc{user_acc = NewUserAcc}})
    end.


get_bookmark(Opts) ->
    case lists:keyfind(bookmark, 1, Opts) of
        {_, BM} when is_list(BM), BM /= [] ->
            BM;
        _ ->
            []
    end.


update_bookmark(CAcc, Hits) ->
    BM = CAcc#cacc.bookmark,
    NewBM = hastings_bookmark:update(BM, Hits),
    CAcc#cacc{bookmark = NewBM}.


pack_bookmark([]) ->
    [];
pack_bookmark(Bookmark) ->
    hastings_bookmark:pack(Bookmark).


unpack_bookmark(Opts) ->
    NewBM = case lists:keyfind(bookmark, 1, Opts) of
        {_, nil} ->
            [];
        {_, Bin} ->
            try
                hastings_bookmark:unpack(Bin)
            catch _:_ ->
                ?MANGO_ERROR({invalid_bookmark, Bin})
            end
    end,
    lists:keystore(bookmark, 1, Opts, {bookmark, NewBM}).


ddocid(Idx) ->
    case mango_idx:ddoc(Idx) of
        <<"_design/", Rest/binary>> ->
            Rest;
        Else ->
            Else
    end.


update_hastings_args(CAcc) ->
    #cacc{
        bookmark = Bookmark,
        h_args = HQArgs
    } = CAcc,
    HQArgs#h_args{
        bookmark = Bookmark,
        limit = get_limit(CAcc)
    }.


get_limit(CAcc) ->
    erlang:min(get_hastings_limit(), CAcc#cacc.limit + CAcc#cacc.skip).


get_hastings_limit() ->
    config:get_integer("hastings", "max_limit", 200).


add_docs(DbName, Hits) ->
    DocIds = [Id || #h_hit{id=Id} <- Hits],
    {ok, Docs} = hastings_util:get_json_docs(DbName, DocIds),
    lists:map(fun(H) ->
        {_, Doc} = lists:keyfind(H#h_hit.id, 1, Docs),
        H#h_hit{doc = Doc}
    end, Hits).
