% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mango_httpd_handlers).

-export([url_handler/1, db_handler/1, design_handler/1, endpoints/1]).

url_handler(_) -> no_match.

db_handler(<<"_index">>)        -> fun mango_httpd:handle_req/2;
db_handler(<<"_explain">>)      -> fun mango_httpd:handle_req/2;
db_handler(<<"_find">>)         -> fun mango_httpd:handle_req/2;
db_handler(_) -> no_match.

design_handler(_) -> no_match.

endpoints(url_handler) ->
    [];
endpoints(db_handler) ->
    [
        <<"_index">>,
        <<"_explain">>,
        <<"_find">>
    ];
endpoints(design_handler) ->
    [].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

mango_endpoints_test_() ->
    Apps = [couch_epi, mango],
    chttpd_httpd_handlers_test_util:endpoints_test(mango, ?MODULE, Apps).

-endif.
