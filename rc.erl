-module(rc).
-export([rctm/1]).
-include_lib("eunit/include/eunit.hrl").

-record(state, {
                name,  % name of the tm
                store, % actual kv store
                dirty, % a "set" of {keys} (only 1 tx can possibly be modifying it at any time)
                pending, % a dict of txid:[{key, value}], what are the pending writes for a particular tx
                waiting % a set of pending write operations (because of blocking of other txs) each marked with the actual txid of the blocker
               }).

%% Read Committed.

%% While the generic public name of this isolation protocol is Read Committed, it is not clear from the name what exactly has to be done.

%% What has to be done is this:
% Reads should only read committed data (and changes made in our own transaction)
% Writes should block. At a time, for each data item, there can be only one transaction attempting to write. Attempting is key, as the write will only complete after the transaction commits and the blocking tx needs to wait for this.

rctm(State) ->
    receive
        % simple read
        {read, Sender, Key} ->
            read(State, Sender, none, Key);
        % simple write
        {write, Sender, Key, Value} ->
            rctm(write(State, Sender, none, Key, Value));
        % begin tx
        {begintx, Sender, _} ->
            Sender ! {begintxresp, State#state.name, true};
        % tx read
        {read, Sender, Txid, Key} ->
            read(State, Sender, Txid, Key);
        % tx write
        {write, Sender, Txid, Key, Value} ->
            rctm(write(State, Sender, Txid, Key, Value));
        % commit tx
        {committx, Sender, Txid} ->
            rctm(commit(State, Sender, Txid));
        % abort tx
        {aborttx, Sender, Txid} ->
            rctm(abort(State, Sender, Txid))
    end,
    rctm(State).

% helper to get a value for a key from a dict and invalid if not found
get_value_or_invalid(Dict, Key) ->
    case orddict:is_key(Key, Dict) of
        true -> orddict:fetch(Key, Dict);
        false -> invalid
    end.

% simple read version
read(State, Sender, none, Key) ->
    Value = get_value_or_invalid(State#state.store, Key),
    Sender ! {readresp, State#state.name, Value};

% read as a part of a tx
% Algo:
% 1. Check if the value has been modified by us -> if so, return that.
% 2. Read the value in store.
% SOLVEME
read(State, Sender, Txid, Key) ->
    % check if our txid has any modifications
    Value = case orddict:is_key(Txid, State#state.pending) of
                false -> get_value_or_invalid(State#state.store, Key);
                true -> element(2, lists:keyfind(Key, 1, orddict:fetch(Txid, State#state.pending)))
            end,
    Sender ! {readresp, State#state.name, Value}.

% simple write version
write(State, Sender, none, Key, Value) ->
    NewStore = orddict:store(Key, Value, State#state.store),
    Sender ! {writeresp, State#state.name},
    State#state{store = NewStore};

% write as a part of a tx
% Algo:
% 1. Check if the key that we are trying to write is dirty.
%    If so, add the write function in a pending queue
%    (The write would be called again later by the blocker committing)
% 2. Else, store the write in the dirty set.
write(State, _, Txid, Key, Value) ->
    % 2. Write the key-value to pending.
    Wstate = case get_value_or_invalid(State#state.pending, Txid) of
                 invalid -> [];
                 X -> X
             end,
    State#state{pending = orddict:store(Txid, Wstate ++ [{Key, Value}], State#state.pending)}.

% helper to update a set of key-values in a store
update_store(Store, []) ->
    Store;
update_store(Store, [{K, V}]) ->
    orddict:store(K, V, Store);
update_store(Store, [{K, V} | Pending]) ->
    update_store(orddict:store(K, V, Store), Pending).

% commit a tx
% Algo:
% 1. Check if any of our commands is blocked in the pending state.
%    If so, return false. If not continue.
% 2. Update the store with all entries in pending related to Txid
%    Also remove them from the pending array.
% 3. Inform the Sender of true commit.
% 4. Go through pending array and trigger pending operations.
commit(State, Sender, Txid) ->
    % 2. Move kvs from pending to store.
    {NewStore, NewPending} = case get_value_or_invalid(State#state.pending, Txid) of
                   % no pending writes
                   invalid -> {State#state.store, State#state.pending};
                   P -> {update_store(State#state.store, P), orddict:erase(Txid, State#state.pending)}
               end,
    % 3
    Sender ! {committxresp, State#state.name, true},
    State#state{store=NewStore, pending=NewPending}.

% Abort an ongoing tx.
% Algo:
% 1. Remove any entries of ours in the waiting array
%    (we don't need them anymore)
% 2. Remove any pending entries under our txid.
% 3. Signal sender of successful abort.
% 4. Trigger any commands waiting on us in the waiting array
abort(State, Sender, Txid) ->
    % 2. Clean our pending
    NewPending = orddict:erase(Txid, State#state.pending),
    % 3
    Sender ! {aborttxresp, State#state.name, true},
    State#state{pending=NewPending}.

% This starts a new transaction manager process used for subsequent operation.
% SOLVEME
start_tm(UniqueId) ->
    register(UniqueId, spawn(rc, rctm, [#state{name=UniqueId, store=orddict:new(), dirty=[], pending=orddict:new(), waiting=[]}])).

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
      fun tm:tx_1_test/0,
      fun tm:tx_2_test/0,
      fun tm:tx_3_test/0,
      fun tm:tx_4_test/0,
      fun tm:tx_5_test/0,
      % isolation
      fun tm:g0_test/0
     ]}.
