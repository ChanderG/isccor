-module(tm).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%% WIP
% Once done, remove
% the solution section
% the export of the solution section
% all function bodies that have SOLVEME in the comments above.
% comment out all test bodies

%% Transaction Manager API and tests.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stage 1: Spawning the TM and test setup.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup(_) ->
    exit(whereis(tm),kill),
    timer:sleep(10). % give it a little time to die

start_tm_test() ->
    ?assert(whereis(tm) =/= undefined).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stage 2: Simple reads and writes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Now we try to get simple serial execution working.

% The TM will store data of the form (Key, Value). As we have no
% particular demands of performance, this can be done using any
% suitable data structure such as dict, ordereddict, map, ets etc.

% Read a key from the store. Tm is the Transaction Manager responsible
% for the store. Should return a single value. Should return "invalid"
% if key is not present.
% SOLVEME
read(Tm, Key) ->
    Tm ! {read, self(), Key},
    receive
        {readresp, Tm, Value} -> Value
    end.

% Write Value for Key.
% Be sure to wait for a response from the tm.
% SOLVEME
write(Tm, Key, Value) ->
    Tm ! {write, self(), Key, Value},
    receive
        {writeresp, Tm} -> true
    end.

invalid_read_test() ->
    ?assertEqual(invalid, read(tm, a)).

read_write_0_test() ->
    [
     ?assertEqual(invalid, read(tm, a)),
     write(tm, a, "hello"),
     ?assertEqual("hello", read(tm, a))
    ].

% test multiple writes to the same key
read_write_1_test() ->
    [?assertEqual(invalid, read(tm, a)),
     write(tm, a, "hello"),
     ?assertEqual("hello", read(tm, a)),
     write(tm, a, "bye"),
     write(tm, b, "there"),
     ?assertEqual("bye", read(tm, a)),
     ?assertEqual("there", read(tm, b))].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stage 3: Transactions (serially)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Now, we are getting into business. Simple reads and writes
% are atomic transactions on their own, there is no role of
% isolation anywhere.

% In this section, we want to implement transactions, each with
% multiple reads/write commands. For now, we will consider
% transactions running sequentially, so no need to worry about
% isolation in this section as well. No need to secure the tm against
% concurrent transactions, our tests will take care to send in
% commands of a single transaction at a time.

% Normally, a tm itself assigns transaction ids, but we will simplify
% and instead send in txids from outside.

% First for any transaction, we need to be able to begin it and the
% after a sequence of commands, we need to be able to commit or
% rollback it. This is the "Atomic" part of ACID. We will not worry
% about rollback for now.

% Begin a tx. Respond with a bool ack of being able to start
% a new tx or not.
% SOLVEME.
begin_tx(Tm, Txid) ->
    Tm ! {begintx, self(), Txid},
    receive
        {begintxresp, Tm, Status} -> Status
    end.

% Then we have to commit a tx. At this point, the tm should
% make it's changes permanent.

% For a RU system, it seems to be a trivial approach of not
% doing anything now and having each command display it's
% changes.

% Respond with a bool where true represent commit and false
% means that the tm was unable to commit this Tx.
commit_tx(Tm, Txid) ->
    Tm ! {committx, self(), Txid},
    receive
        {committxresp, Tm, Status} -> Status
    end.

tx_0_test() ->
    [?assert(begin_tx(tm, txid1)),
     ?assert(commit_tx(tm, txid1))].

% look at that!!!, we are able to commit a non-existing tx
% never mind, this is not of importance for this objective
% just leaving this here to show how incomplete our simulation is
tx_1_test() ->
    ?assert(commit_tx(tm, txid2)).

% Updated version of read and write above, now also passing in the
% txid.
% HINT: From what we know of Read Uncommited so far, does the
% Txid really matter?

% SOLVEME
read(Tm, Txid, Key) ->
    read(Tm, Key).

% SOLVEME
write(Tm, Txid, Key, Value) ->
    write(Tm, Key, Value).

% Let's test our new transaction semantics.
tx_2_test() ->
    [
     ?assert(begin_tx(tm, txid1)),
     write(tm, txid1, a, "hello"),
     ?assertEqual("hello", read(tm, txid1, a)),
     ?assert(commit_tx(tm, txid1)),
     ?assert(begin_tx(tm, txid2)),
     write(tm, txid2, a, "there"),
     ?assertEqual("there", read(tm, txid2, a)),
     ?assert(commit_tx(tm, txid2))
    ].

% So far, nothing much. All of this is trivial right?

% Now for the actual concurrency control!!!

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stage 4: Transactions (concurrently)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% This is crux of the matter. We need to test the system against the
% known weak isolation level concurrency anomalies.

% Basic
% Read Uncommitted test. Check if we are able to read uncommitted
% values of other txs.

tx_3_test() ->
    [
     ?assert(begin_tx(tm, t1)),
     ?assert(begin_tx(tm, t2)),
     ?assertEqual(invalid, read(tm, t1, a)),
     write(tm, t1, a, "hello"),
     ?assertEqual("hello", read(tm, t1, a)),
     ?assertEqual("hello", read(tm, t2, a)),
     write(tm, t2, a, "there"),
     ?assertEqual("there", read(tm, t2, a)),
     ?assert(commit_tx(tm, t1)),
     ?assert(commit_tx(tm, t2))
    ].

%% G0: Write Cycles.

g0_test() ->
    [
     ?assert(begin_tx(tm, t1)),
     write(tm, t1, 1, 11),
     write(tm, t2, 1, 12),
     write(tm, t1, 2, 21),
     ?assert(commit_tx(tm, t1)),
     ?assertEqual(11, read(tm, 1)),
     ?assertEqual(21, read(tm, 2)),
     write(tm, t2, 2, 22),
     ?assert(commit_tx(tm, t2)),
     ?assertEqual(12, read(tm, 1)),
     ?assertEqual(22, read(tm, 2))
    ].
