-module(si).
-export([sitm/1]).
-include_lib("eunit/include/eunit.hrl").

-record(state, {
                name,
                store,
                snapshots,
                writereadsets
               }).

%% Snapshot Isolation

% We will attempt Snapshot Isolation using MVCC, that is holding
% multiple versions of value for a single key. Predicate Locking based
% approach is another, we will deal with that later.

% Put simply, every transaction takes a snapshot of the database when
% is starts. Then, it makes changes to the local copy as needed. At
% commit time, it sees if the changes it has made impacts any other
% changes, if so, it aborts the tx.

% Since, we have the luxury of duplicating the entire database,
% without worrying about performance, we will use it to our
% advantage. The only other thing you need is a way of tracking which
% keys have been written to by txs other than any given tx.

% The flow of the tm itself would be similar to the other tms.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Prerequisite: Snapshot mechanism
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Let's focus on the Snapshot part first.

%%%%%%%%%%%%%%%%%%%%%%%%%%
% Snapshot: Part A: Creating
%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Firstly, we need to be able to take a snapshot for reading purposes.
% This, for us can be a simple copy of the kv store.
% SOLVEME
create_snapshot(Global) ->
    Global.

create_snapshot_test() ->
    Global = orddict:new(),
    Global1 = orddict:store(a, 1, Global),
    ?assertEqual(1, orddict:fetch(a, Global1)),
    Snapshot = create_snapshot(Global1),
    _ = orddict:store(a, 2, Global1),
    ?assertEqual(1, orddict:fetch(a, Snapshot)).

%%%%%%%%%%%%%%%%%%%%%%%%%%
% Snapshot: Part B: Reading and writing
%%%%%%%%%%%%%%%%%%%%%%%%%%

% We need to be able to work on our snapshot. Reading is
% straightforward.
% SOLVEME
read_snapshot(Snapshot, Key) ->
    case orddict:is_empty(Snapshot) of
        true -> invalid;
        false -> case orddict:is_key(Key, Snapshot) of
                     true -> orddict:fetch(Key, Snapshot);
                     false -> invalid
                 end
    end.

read_snapshot_0_test()->
    Global = orddict:new(),
    Global1 = orddict:store(a, 1, Global),
    Snapshot1 = create_snapshot(Global1),
    ?assertEqual(1, read_snapshot(Snapshot1, a)),
    ?assertEqual(invalid, read_snapshot(Snapshot1, b)).

% Writing is a bit problematic. If we overwrite our
% snapshot, we will lose track of what keys we wrote and diffing
% (below) will become a problem.

% So, we will have to maintain a set of keys and values that we write
% over.  Then, we can use this set to check if the global store has
% changed since the time of our snapshot. WriteReadset can be modelled
% as a dictionary of key and value.  Multple writes to the same key
% should not update the writereadset. This way, the Snapshot itself can be
% safetly updated.
% SOLVEME
write_snapshot(Snapshot, WriteReadset, Key, Value) ->
    NewWriteReadset = case orddict:is_key(Key, WriteReadset) of
                          true ->
                              % we have prev updated the same key
                              WriteReadset;
                          false ->
                              % first time changing this key
                              orddict:store(Key, read_snapshot(Snapshot, Key), WriteReadset)
                      end,
    NewSnapshot = orddict:store(Key, Value, Snapshot),
    {NewSnapshot, NewWriteReadset}.

write_snapshot_test() ->
    Snapshot = orddict:new(),
    {Snapshot1, WriteReadset1} = write_snapshot(Snapshot, orddict:new(), a, 1),
    ?assertEqual(1, read_snapshot(Snapshot1, a)),
    ?assertEqual(invalid, read_snapshot(WriteReadset1, a)),
    {Snapshot2, WriteReadset2} = write_snapshot(Snapshot1, WriteReadset1, a, 2),
    ?assertEqual(2, read_snapshot(Snapshot2, a)),
    ?assertEqual(invalid, read_snapshot(WriteReadset2, a)).

%%%%%%%%%%%%%%%%%%%%%%%%%%
% Snapshot: Part B: Merging
%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Then, we need to track writes.
% Following above, the simple solution would be to just write to our local copy,
% Then, we will need a way to merge our copy with the global copy.
% Merge here is simple:
% 1. For all keys in our snapshot, if the value is different from the Global value,
%    use our value.
% 2. If they are same, do nothing.

% SOLVEME
merge(Global, Snapshot) ->
    case orddict:is_empty(Snapshot) of
        true -> Global;
        false ->
            % fetch first key and move it
            Key = lists:nth(1, orddict:fetch_keys(Snapshot)),
            NewGlobal = orddict:store(Key, read_snapshot(Snapshot, Key), Global),
            NewSnapshot = orddict:erase(Key, Snapshot),
            merge(NewGlobal, NewSnapshot)
    end.

% test the merge operation
merge_test() ->
    Global = orddict:new(),
    Global1 = orddict:store(a, 1, Global),
    Snapshot1 = create_snapshot(Global1),
    {Snapshot2, _} = write_snapshot(Snapshot1, orddict:new(), a, 2),
    {Snapshot3, _} = write_snapshot(Snapshot2, orddict:new(), b, 1),
    Global2 = merge(Global1, Snapshot3),
    ?assertEqual(2, orddict:fetch(a, Global2)),
    ?assertEqual(1, orddict:fetch(b, Global2)).

%%%%%%%%%%%%%%%%%%%%%%%%%%
% Snapshot: Part C: Diffing
%%%%%%%%%%%%%%%%%%%%%%%%%%

% Other txs may have committed during our operation.
% Hence, we need to Diff with the global value to see if things have changed.

% Algorithm for can_merge(Global, WriteReadSet)
% 1. For all keys in WriteReadSet, check if values match in Global and WriteReadSet.
%    If so, return true.
% 2. If not, return false.

% SOLVEME
can_merge(Global, WriteReadset) ->
    Mismatch = orddict:filter(fun(K, V) ->
                                      V =/= read_snapshot(Global, K)
                              end, WriteReadset),
    orddict:is_empty(Mismatch).

can_merge_0_test() ->
    Global = orddict:new(),
    Global1 = orddict:store(a, 1, Global),
    ?assert(can_merge(Global1, orddict:new())).

can_merge_1_test() ->
    Global = orddict:new(),
    Global1 = orddict:store(a, 1, Global),
    Snapshot1 = create_snapshot(Global1),
    {_, WRS2} = write_snapshot(Snapshot1, orddict:new(), a, 2),
    % snapshot has updated a = 2, global has not changed
    ?assert(can_merge(Global1, WRS2)),
    Global2 = orddict:store(b, 1, Global1),
    % global has updated b, a value not related to the snapshot
    ?assert(can_merge(Global2, WRS2)),
    Global3 = orddict:store(a, 3, Global2),
    % global has updated a which is a direct conflict
    ?assertNot(can_merge(Global3, WRS2)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Actual Snapshot Isolation based TM.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sitm(State) ->
    Name = State#state.name,
    receive
        % simple reads and writes
        {read, Sender, Key} ->
            Sender ! {readresp, Name, read_snapshot(State#state.store, Key)};
        {write, Sender, Key, Value} ->
            Sender ! {writeresp, Name},
            sitm(State#state{store=orddict:store(Key, Value, State#state.store)});
        % Tx semantics
        {begintx, Sender, Txid} ->
            sitm(begin_tx(State, Sender, Txid));
        {read, Sender, Txid, Key} ->
            read(State, Sender, Txid, Key);
        {write, Sender, Txid, Key, Value} ->
            sitm(write(State, Sender, Txid, Key, Value));
        {committx, Sender, Txid} ->
            sitm(commit_tx(State, Sender, Txid));
        {aborttx, Sender, Txid} ->
            sitm(abort_tx(State, Sender, Txid))
    end,
    sitm(State).

% Begin a tx.
begin_tx(State, Sender, Txid) ->
    Sender ! {begintxresp, State#state.name, true},
    CurrSnapshots = State#state.snapshots,
    CurrWRSets = State#state.writereadsets,
    State#state{
      snapshots=orddict:store(Txid, create_snapshot(State#state.store), CurrSnapshots),
      writereadsets=orddict:store(Txid, orddict:new(), CurrWRSets)
    }.

% Read (command in a tx)
read(State, Sender, Txid, Key) ->
    Sender ! {readresp, State#state.name, read_snapshot(orddict:fetch(Txid, State#state.snapshots), Key)}.

% Write (command in a tx)
write(State, _, Txid, Key, Value) ->
    {NewSnapshot, NewWRSet} = write_snapshot(
                                orddict:fetch(Txid, State#state.snapshots),
                                orddict:fetch(Txid, State#state.writereadsets),
                                Key,
                                Value),
    State#state{
      snapshots=orddict:store(Txid, NewSnapshot, State#state.snapshots),
      writereadsets=orddict:store(Txid, NewWRSet, State#state.writereadsets)
     }.

% Commit a tx.
% 1. Check if we can_merge this snapshot back to store.
%    If so, merge the snapshot, send true to client.
% 2. If not, send false to the client.
% 3. Clean out the snapshots and writereadsets of this Tx's entries.
commit_tx(State, Sender, Txid) ->
    {UpdatedStore, Status} = case can_merge(State#state.store, orddict:fetch(Txid, State#state.writereadsets)) of
                                 true ->
                                     {merge(State#state.store, orddict:fetch(Txid, State#state.snapshots)), true};
                                 false ->
                                     {State#state.store, false}
                             end,
    Sender ! {committxresp, State#state.name, Status},
    UpdatedSnapshots = orddict:erase(Txid, State#state.snapshots),
    UpdatedWRSets = orddict:erase(Txid, State#state.writereadsets),
    State#state{store=UpdatedStore, snapshots=UpdatedSnapshots, writereadsets=UpdatedWRSets}.

% Simply drop the snapshots
abort_tx(State, Sender, Txid) ->
    Sender ! {aborttxresp, State#state.name, true},
    UpdatedSnapshots = orddict:erase(Txid, State#state.snapshots),
    UpdatedWRSets = orddict:erase(Txid, State#state.writereadsets),
    State#state{snapshots=UpdatedSnapshots, writereadsets=UpdatedWRSets}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% TM Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% This starts a new transaction manager process used for subsequent operation.
% SOLVEME
start_tm(UniqueId) ->
    register(UniqueId, spawn(si, sitm, [#state{name=UniqueId, store=orddict:new(), snapshots=orddict:new(), writereadsets=orddict:new()}])).

%% Test setup
setup() ->
    start_tm(tm).

%% Teardown is common to all options.

main_test_() ->
    {foreach,
     fun setup/0,
     fun tm:cleanup/1,
     [
      fun tm:start_tm_test/0,
      % test reads/writes,
      fun tm:invalid_read_test/0,
      fun tm:read_write_0_test/0,
      fun tm:read_write_1_test/0,
      % simple tx tests
      fun tm:tx_0_test/0,
      fun tm:tx_2_test/0,
      fun tm:tx_3_test/0,
      fun tm:tx_4_test/0,
      fun tm:tx_5_test/0,
      % test for anomalies
      %% fun tm:g0_test/0,
      %% fun tm:g1a_test/0,
      %% fun tm:g1b_test/0,
      %% fun tm:g1c_test/0
      %% fun tm:g_single_test()
     ]}.
