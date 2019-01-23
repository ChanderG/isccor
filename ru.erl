-module(ru).
-export([start_tm/1, tmanager/2]).
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

%% Solution section begins.

% actual transaction manager code.
tmanager(Name, KVStore) ->
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
            tmanager(Name, NewKVStore);
        {begintx, Sender, _} ->
            % We simply start all txs.
            Sender ! {begintxresp, Name, true};
        {committx, Sender, _} ->
            % We simply finish up. This is read uncommitted;
            % so as such tx changes are not isolated...
            Sender ! {committxresp, Name, true}
    end,
    tmanager(Name, KVStore).

%% Solution section ends.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stage 1: Spawning the TM and test setup.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Test fixtures.

setup() ->
    start_tm(tm).

cleanup(_) ->
    exit(whereis(tm), kill),
    timer:sleep(10). % some time for the tm to exit

testme(Test) ->
    {setup, fun setup/0, fun cleanup/1, [ Test ]}.

%% Now to run all tests, uses ru:test().
%% Or to run a specific test, use eunit:test(ru:testname)

% This starts a new transaction manager process used for subsequent operation.
% SOLVEME
start_tm(UniqueId) ->
    register(UniqueId, spawn(ru, tmanager, [UniqueId, orddict:new()])).

start_tm_test_() ->
    testme(?_test(?assert(whereis(tm) =/= undefined))).

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

invalid_read_test_() ->
    testme(?_test(
              ?assertEqual(invalid, read(tm, a))
             )).

read_write_0_test_() ->
    testme(?_test([
                   ?assertEqual(invalid, read(tm, a)),
                   write(tm, a, "hello"),
                   ?assertEqual("hello", read(tm, a))
                  ])).

% test multiple writes to the same key
read_write_1_test_() ->
    testme(?_test([
                   ?assertEqual(invalid, read(tm, a)),
                   write(tm, a, "hello"),
                   ?assertEqual("hello", read(tm, a)),
                   write(tm, a, "bye"),
                   write(tm, b, "there"),
                   ?assertEqual("bye", read(tm, a)),
                   ?assertEqual("there", read(tm, b))
                  ])).

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

tx_0_test_() ->
    testme(?_test([
                   ?assert(begin_tx(tm, txid1)),
                   ?assert(commit_tx(tm, txid1))
                  ])).

% look at that!!!, we are able to commit a non-existing tx
% never mind, this is not of importance for this objective
% just leaving this here to show how leaky our abstraction is
tx_1_test_() ->
    testme(?_test([
                   ?assert(commit_tx(tm, txid2))
                  ])).

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
tx_2_test_() ->
    testme(?_test([
                   ?assert(begin_tx(tm, txid1)),
                   write(tm, txid1, a, "hello"),
                   ?assertEqual("hello", read(tm, txid1, a)),
                   ?assert(commit_tx(tm, txid1)),
                   ?assert(begin_tx(tm, txid2)),
                   write(tm, txid2, a, "there"),
                   ?assertEqual("there", read(tm, txid2, a)),
                   ?assert(commit_tx(tm, txid2))
                  ])).

% So far, nothing much. All of this is trivial right?

% Now for the actual concurrency control!!!
