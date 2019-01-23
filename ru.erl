-module(ru).
-export([rutm/2]).
-include_lib("eunit/include/eunit.hrl").

%% WIP
% Once done, remove
% the solution section
% the export of the solution section
% all function bodies that have SOLVEME in the comments above.
% comment out all test bodies

%% Read Uncommitted Concurrency Control

%% In this module, we will model the "Read Uncommitted" technique.
%% To be clear, this is more of a "lack of technique" than anything else.
%% We simply run the input commands in order, not caring for protecting anything.
%% As such, we do not have to even think about transactions.

% actual transaction manager code.
rutm(Name, KVStore) ->
    receive
        % single read
        {read, Sender, Key} ->
            Value = case orddict:is_key(Key, KVStore) of
                        true -> orddict:fetch(Key, KVStore);
                        false -> invalid
                    end,
            Sender ! {readresp, Name, Value};
        % single write
        {write, Sender, Key, Value} ->
            NewKVStore = orddict:store(Key, Value, KVStore),
            Sender ! {writeresp, Name},
            rutm(Name, NewKVStore);
        {begintx, Sender, _} ->
            % We simply start all txs.
            Sender ! {begintxresp, Name, true};
        {committx, Sender, _} ->
            % We simply finish up. This is read uncommitted;
            % so as such tx changes are not isolated...
            Sender ! {committxresp, Name, true}
    end,
    rutm(Name, KVStore).

%% Now to run all tests, uses ru:test(). Add tests from tm.erl to the
%% list below as needed.

% This starts a new transaction manager process used for subsequent operation.
% SOLVEME
start_tm(UniqueId) ->
    register(UniqueId, spawn(ru, rutm, [UniqueId, orddict:new()])).

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
      fun tm:start_tm_test_/0,
      % test reads/writes
      fun tm:invalid_read_test_/0,
      fun tm:read_write_0_test_/0,
      fun tm:read_write_1_test_/0,
      % test single txs
      fun tm:tx_0_test_/0,
      fun tm:tx_1_test_/0,
      fun tm:tx_2_test_/0,
      fun tm:tx_3_test_/0
      % test for concurrency anomalies
      %% fun tm:g0_test_/0
     ]}.
