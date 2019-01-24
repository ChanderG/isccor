-module(rc).
-export([rctm/2]).
-include_lib("eunit/include/eunit.hrl").

%% Read Committed.

%% While the generic public name of this isolation protocol is Read Committed, it is not clear from the name what exactly has to be done.

%% What has to be done is this:
% Reads should only read committed data (and changes made in our own transaction)
% Writes should block. At a time, for each data item, there can be only one transaction attempting to write. Attempting is key, as the write will only complete after the transaction commits and the blocking tx needs to wait for this.

rctm(Name, KVStore) ->
    receive
        % simple read
        {read, Sender, Key} ->
            Value = case orddict:is_key(Key, KVStore) of
                        true -> orddict:fetch(Key, KVStore);
                        false -> invalid
                    end,
            Sender ! {readresp, Name, Value};
        % simple write
        {write, Sender, Key, Value} ->
            NewKVStore = orddict:store(Key, Value, KVStore),
            Sender ! {writeresp, Name},
            rctm(Name, NewKVStore);
        % begin tx
        {begintx, Sender, _} ->
            Sender ! {begintxresp, Name, true};
        % commit tx
        {committx, Sender, _} ->
            Sender ! {committxresp, Name, true}
    end,
    rctm(Name, KVStore).

% This starts a new transaction manager process used for subsequent operation.
% SOLVEME
start_tm(UniqueId) ->
    register(UniqueId, spawn(rc, rctm, [UniqueId, orddict:new()])).

%% Test setup
setup() ->
    start_tm(tm).

%% Teardown is common to all options.

%% Test manager: add and control the tests you want to run.

main_test_() ->
    {foreach,
     fun setup/0,
     fun tm:cleanup/1,
     [
      % to ensure tm starts
      fun tm:start_tm_test/0,
      % test reads/writes
      fun tm:invalid_read_test/0,
      fun tm:read_write_0_test/0,
      fun tm:read_write_1_test/0,
      % test tx semantics
      fun tm:tx_0_test/0,
      fun tm:tx_1_test/0
     ]}.
