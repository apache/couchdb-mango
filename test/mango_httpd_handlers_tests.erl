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

-module(mango_httpd_handlers_tests).

-export([handlers/1]).

-include_lib("couch/include/couch_eunit.hrl").

handlers(url_handler) ->
    [];
handlers(db_handler) ->
    [
        {<<"_index">>, mango_httpd, handle_req},
        {<<"_explain">>, mango_httpd, handle_req},
        {<<"_find">>, mango_httpd, handle_req}
    ];
handlers(design_handler) ->
    [].

mango_endpoints_test_() ->
    Apps = [couch_epi, mango],
    chttpd_httpd_handlers_test_util:endpoints_test(mango, ?MODULE, Apps).
