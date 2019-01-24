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

%% In this module, we will (kinda) model the "Read Uncommitted" technique.
%% To be clear, this is more of a "lack of technique" than anything else.
%% We simply run the input commands in order, not caring for protecting anything.
%% As such, we do not have to even think about transactions. So here,
%% we will not really deal with aborts.

% actual transaction manager code.
rutm(Name, KVStore) ->
    receive
        % single read
        {read, Sender, Key} ->
            rutm_read(Name, KVStore, Sender, Key);
        % single write
        {write, Sender, Key, Value} ->
            rutm(Name, rutm_write(Name, KVStore, Sender, Key, Value));
        {begintx, Sender, _} ->
            % We simply start all txs.
            Sender ! {begintxresp, Name, true};
        {committx, Sender, _} ->
            % We simply finish up. This is read uncommitted;
            % so as such tx changes are not isolated...
            Sender ! {committxresp, Name, true};
        {read, Sender, Txid, Key} ->
            rutm_read(Name, KVStore, Sender, Key);
        {write, Sender, Txid, Key, Value} ->
            rutm(Name, rutm_write(Name, KVStore, Sender, Key, Value))
    end,
    rutm(Name, KVStore).

rutm_read(Name, KVStore, Sender, Key) ->
    Value = case orddict:is_key(Key, KVStore) of
                true -> orddict:fetch(Key, KVStore);
                false -> invalid
            end,
    Sender ! {readresp, Name, Value}.

rutm_write(Name, KVStore, Sender, Key, Value) ->
    NewKVStore = orddict:store(Key, Value, KVStore),
    Sender ! {writeresp, Name},
    NewKVStore.

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
      fun tm:start_tm_test/0,
      % test reads/writes
      fun tm:invalid_read_test/0,
      fun tm:read_write_0_test/0,
      fun tm:read_write_1_test/0,
      % test single txs
      fun tm:tx_0_test/0,
      fun tm:tx_1_test/0,
      fun tm:tx_2_test/0,
      fun tm:tx_3_test/0,
      fun tm:tx_4_test/0,
      % we cannot really do abort here, can we?
      % fun tm:tx_5_test/0,
      % test for concurrency anomalies
      fun tm:tx_con_ru_test/0
      %% fun tm:g0_test/0
     ]}.
