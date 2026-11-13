-------------------------- MODULE OsirisMsgPassing --------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS R, \* The set of all replicas
          V \* The set of all message payloads that can be sent

VARIABLES
          \* state stored in each replica
          rep_state,            \* the state of each replica
          rep_epoch,            \* the epoch of each replica
          rep_leader,           \* the replica each replica believes is leader
          rep_log,              \* the log of each replica
          rep_ler,              \* the Log End Record of each replica
          rep_max_ler_of_rep,   \* for each replica, what it knows to be the LER of every other replica
          rep_listener,         \* registered listeners
          rep_committed_offset, \* the committed offset known to each replica
          rep_first_offset,     \* the first (lowest) offset still in the log after retention

          \* state stored in the coordinator
          coord_state,          \* the state of each replica, according to the coordinator
          coord_epoch,          \* the current epoch
          coord_leader,         \* the replica that was elected leader
          coord_election,       \* the election phase of each epoch
          coord_election_ler,   \* the LER of each fenced replica, per epoch

          \* meta data required by the spec
          confirmed,            \* the confirmation status of each written value
          confirmed_replicas,   \* the set of replicas that had the value at confirmation time
          start_stop_ctr,       \* a counter to limit the state space
          requests,             \* the set of all requests and their responses
          responses_processed   \* the set of all processed responses

AllVars == << rep_state, rep_epoch, rep_leader, rep_log, rep_ler, rep_max_ler_of_rep, rep_listener,
              rep_committed_offset, rep_first_offset,
              coord_state, coord_epoch, coord_leader, coord_election, coord_election_ler,
              confirmed, confirmed_replicas, start_stop_ctr, requests, responses_processed >>
RepVars == << rep_state, rep_epoch, rep_leader, rep_log, rep_ler, rep_max_ler_of_rep, rep_listener,
              rep_committed_offset, rep_first_offset >>
CoordVars == << coord_state, coord_epoch, coord_leader, coord_election, coord_election_ler >>

Record == [offset: Nat, epoch: Nat, value: V]
NoRecord == [offset |-> 0, epoch |-> 0]
MetaOffset == {-2, -1 } \union Nat
MetaRecord == [offset: MetaOffset, epoch: Nat]
NoMetaRecord == [offset |-> 0, epoch |-> 0]

\* Choose one of the leaders to start as the leader. This models the first start-up of the cluster
\* where all replicas are empty and any are a candidate for leader, so no election required.
InitialLeader ==
    CHOOSE r \in R : TRUE

Init ==
    /\ rep_state = [r \in R |-> IF r = InitialLeader THEN "leader" ELSE "follower"]
    /\ rep_epoch = [r \in R |-> 1]
    /\ rep_leader = [r \in R |-> InitialLeader]
    /\ rep_log = [r \in R |-> {}]
    /\ rep_ler = [r \in R |-> NoMetaRecord]
    /\ rep_max_ler_of_rep = [r \in R |-> [rr \in R |-> NoMetaRecord]]
    /\ rep_listener = [r \in R |-> [rr \in R |-> FALSE]]
    /\ rep_committed_offset = [r \in R |-> 0]
    /\ rep_first_offset = [r \in R |-> 0]
    /\ coord_state = [r \in R |-> IF r = InitialLeader THEN "leader" ELSE "follower"]
    /\ coord_epoch = 1
    /\ coord_leader = InitialLeader
    /\ coord_election = [e \in {} |-> 0] @@ (1 :> "leader_elected")
    /\ coord_election_ler = [e \in {} |-> 0] @@ (1 :> [r \in R |-> NoMetaRecord])
    /\ confirmed = [w \in {} |-> 0]
    /\ confirmed_replicas = [w \in {} |-> 0]
    /\ start_stop_ctr = 0
    /\ requests = [req \in {} |-> 0]
    /\ responses_processed = {}

\* -----------------------------------------
\* HELPER OPERATORS
\* -----------------------------------------

\* Reusing the meta record for maintaining the fencing status, to avoid more variables
\* a dirty trick but worth avoiding another variable
PendingFencingRequest == [offset |-> -2, epoch |-> 0]
PendingFencingResponse == [offset |-> -1, epoch |-> 0]

\* Send a request only if the request has already not been sent
SendRequest(request) ==
    /\ request \notin DOMAIN requests
    /\ requests' = requests @@ (request :> [type |-> "-"])

\* Send a reply to a request, given the request has been sent
SendReply(request, reply) ==
    /\ request \in DOMAIN requests
    /\ requests' = [requests EXCEPT ![request] = reply]

\* True when a request has not had a reply sent
NotSentReply(request) ==
    /\ request \in DOMAIN requests
    /\ requests[request].type = "-"

\* True when a response has been received and processed
NotProcessedResponse(response) ==
    \/ response.type = "-"
    \/ /\ response.type # "-"
       /\ response \notin responses_processed

\* Signals that the response is processed so it is not processed again
ResponseProcessed(response) ==
    responses_processed' = responses_processed \union { response }

\* The replicas visible to the coordinator
CoordinatorVisibleReplicas ==
    { r \in R : coord_state[r] # "offline" }

\* Does this set of replicas consistitute a quorum?
IsQuorum(replicas) ==
    Cardinality(replicas) >= (Cardinality(R) \div 2) + 1

\* Does this set of replicas consistitute one short of a quorum?
IsQuorumMinusOne(replicas) ==
    Cardinality(replicas) >= (Cardinality(R) \div 2)

\* Compute the committed offset for a leader: the highest offset that is quorum-written.
\* Returns 0 if no offset is quorum-written.
CommittedOffset(leader) ==
    LET quorum_offsets == { offset \in { rec.offset : rec \in rep_log[leader] } :
                             Cardinality({ r \in R : rep_max_ler_of_rep[leader][r].offset >= offset })
                                >= (Cardinality(R) \div 2) + 1 }
    IN IF quorum_offsets = {} THEN 0
       ELSE CHOOSE offset \in quorum_offsets : \A o \in quorum_offsets : offset >= o

\* Compares two records, with epoch taking precedence.
\* Offset only matters when both have the same epoch.
\* When record1 > record2 then 1
\* When record1 = record2 then 0
\* When record1 < record2 then 1
CompareRecords(record1, record2) ==
    IF record1.epoch > record2.epoch THEN 1
    ELSE
        IF record1.epoch = record2.epoch /\ record1.offset > record2.offset THEN 1
        ELSE
            IF record1.epoch = record2.epoch /\ record1.offset = record2.offset THEN 0
            ELSE -1

RemoveAnyListeners(r) ==
    /\ rep_max_ler_of_rep' = [rep_max_ler_of_rep EXCEPT ![r] = [rr \in R |-> NoMetaRecord]]
    /\ rep_listener' = [rep_listener EXCEPT ![r] = [rr \in R |-> FALSE]]

\* -----------------------------------------
\* REPLICA START/STOP AND COORD-REPLICA VISIBILITY
\* -----------------------------------------

\* a replica stopping or becoming unavailable during an election will cause the election
\* to abort
AbortElectionIfInProgress ==
   \/ /\ coord_epoch = 0
      /\ UNCHANGED coord_election
   \/ /\ coord_epoch > 0
      /\ coord_epoch \in DOMAIN coord_election
      /\ \/ /\ coord_election[coord_epoch] = "leader_elected"
            /\ UNCHANGED coord_election
         \/ /\ coord_election[coord_epoch] # "leader_elected"
            /\ coord_election' = [coord_election EXCEPT ![coord_epoch] = "aborted"]

\* The coordinator tells the replica the current epoch, who the current leader is (if any)
\* A replica that is started can only become a follower if it has an empty log
\* Else it must enter the pending_truncate state
CoordinatorResetsReplica(r) ==
    LET new_state == IF rep_ler[r].offset = 0 THEN "follower" ELSE "pending_truncate"
    IN
        /\ rep_state' = [rep_state EXCEPT ![r] = new_state]
        /\ coord_state' = [coord_state EXCEPT ![r] = new_state]
        /\ rep_epoch' = [rep_epoch EXCEPT ![r] = coord_epoch]
        /\ rep_leader' = [rep_leader EXCEPT ![r] = coord_leader]
        /\ rep_committed_offset' = [rep_committed_offset EXCEPT ![r] = 0]
        /\ RemoveAnyListeners(r)

\* An offline replica starts up and is reset
ReplicaStarts ==
    /\ \E r \in R :
        /\ rep_state[r] = "offline"
        /\ coord_state[r] = "offline"
        \* Extra restriction to reduce state space by not allowing starting during elections: start
\*        /\ \/ coord_epoch = 0
\*           \/ /\ coord_epoch # 0
\*              /\ coord_election[coord_epoch] = "leader_elected"
        \* end
        /\ CoordinatorResetsReplica(r)
        /\ start_stop_ctr' = start_stop_ctr + 1
        /\ UNCHANGED << rep_log, rep_ler, rep_first_offset, coord_epoch, coord_leader,
                        coord_election, coord_election_ler, confirmed, requests,
                        confirmed_replicas, responses_processed >>

\* An online replica stops
\* The coordinator is immediately notified. The stopped replica could be the leader.
ReplicaStops ==
    /\ \E r \in R :
        /\ rep_state[r] # "offline"
        \* Extra restriction to reduce state space by not allowing stopping during elections: start
\*        /\ \/ coord_epoch = 0
\*           \/ /\ coord_epoch # 0
\*              /\ coord_election[coord_epoch] = "leader_elected"
        \* end
        /\ rep_state' = [rep_state EXCEPT ![r] = "offline"]
        /\ coord_state' = [coord_state EXCEPT ![r] = "offline"]
        /\ AbortElectionIfInProgress
        /\ \/ /\ coord_leader = r
              /\ coord_leader' = 0
           \/ /\ coord_leader # r
              /\ coord_leader' = coord_leader
    /\ start_stop_ctr' = start_stop_ctr + 1
    /\ UNCHANGED << rep_epoch, rep_leader, rep_log, rep_ler, rep_max_ler_of_rep, rep_listener,
                    rep_committed_offset, rep_first_offset, coord_epoch, coord_election_ler,
                    confirmed, requests, confirmed_replicas, responses_processed >>

\* A network partition causes the coordinator to lose visibility of a replica
\* The replica could be the current leader
CoordinatorLosesVisibilityOfReplica ==
    /\ \E r \in R :
        /\ rep_state[r] # "offline"
        /\ coord_state[r] # "offline"
        \* Extra restriction to reduce state space by not allowing vis loss during elections: start
\*        /\ \/ coord_epoch = 0
\*           \/ /\ coord_epoch # 0
\*              /\ coord_election[coord_epoch] = "leader_elected"
        \* end
        /\ coord_state' = [coord_state EXCEPT ![r] = "offline"]
        /\ AbortElectionIfInProgress
        /\ \/ /\ r = coord_leader
              /\ coord_leader' = 0
           \/ /\ r # coord_leader
              /\ coord_leader' = coord_leader
        /\ start_stop_ctr' = start_stop_ctr + 1
    /\ UNCHANGED << RepVars, coord_epoch, coord_election_ler, confirmed, confirmed_replicas, requests, responses_processed >>

\* The network partition between the coordinator and the replica is resolved
\* The coordinator tells the replica to reset itself
CoordinatorRestoresVisibilityOfReplica ==
    /\ \E r \in R :
        /\ rep_state[r] # "offline"
        /\ coord_state[r] = "offline"
        /\ CoordinatorResetsReplica(r)
        /\ start_stop_ctr' = start_stop_ctr + 1
    /\ UNCHANGED << rep_log, rep_ler, rep_first_offset,
                    coord_leader, coord_epoch, coord_election, coord_election_ler,
                    confirmed, requests, confirmed_replicas, responses_processed >>

\* ------------------------------------------
\* LEADER ELECTION
\* ------------------------------------------

\* The coordinator sees there is no current leader
\* Having no leader triggers an election if an election process is not ingoing or
\* it has reached the last phase.
\* A replica chosen to be leader that dies before responding is not currently modelled and is
\* a liveness property
CoordinatorTriggersElection ==
    \* Trigger election when there is no leader and election phase not pre leader_elected
    /\ coord_leader = 0
    /\ \/ coord_epoch = 0
       \/ /\ coord_epoch > 0
          /\ coord_election[coord_epoch] \in {"leader_elected", "aborted"}
    /\ IsQuorum(CoordinatorVisibleReplicas)
    \* Actions
    /\ LET new_epoch == coord_epoch + 1
       IN  /\ coord_epoch' = new_epoch
           /\ coord_election' = coord_election @@ (new_epoch :> "fencing")
           /\ coord_election_ler' = coord_election_ler @@
                                        (new_epoch :> [r \in R |-> PendingFencingRequest])
           /\ UNCHANGED << RepVars, coord_state, coord_leader, confirmed, confirmed_replicas,
                           start_stop_ctr, requests, confirmed_replicas, responses_processed >>

\* The first phase of leader election is to fence a quorum of replicas
\* A fenced replica will stop any replication with a stale leader
\* The coordinator sends a replica a fencing request
CoordinatorSendsFencingRequest ==
    /\ coord_epoch > 0
    /\ coord_election[coord_epoch] = "fencing"
    /\ \E r \in R :
        /\ coord_state[r] # "offline"
        /\ coord_election_ler[coord_epoch][r] = PendingFencingRequest
        /\ coord_election_ler' = [coord_election_ler EXCEPT ![coord_epoch] =
                                    [@ EXCEPT ![r] = PendingFencingResponse]]
        /\ SendRequest([type   |-> "fencing_request",
                        epoch  |-> coord_epoch,
                        dest   |-> r])
    /\ UNCHANGED << RepVars, coord_state, coord_epoch, coord_leader, coord_election,
                    confirmed, start_stop_ctr, confirmed_replicas, responses_processed >>

\* A replica processes a fencing requess, fences itself and responds with its LER
\* If the replica has started and not yet truncated its log it can still fence, even
\* though it might have diverged, because it cannot be elected as epoch takes precedence.
ReplicaHandlesFencingRequest ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "fencing_request"
        /\ rep_state[request.dest] # "offline"
        /\ rep_epoch[request.dest] < request.epoch
        \* Actions
        /\ rep_epoch' = [rep_epoch EXCEPT ![request.dest] = request.epoch]
        /\ rep_state' = [rep_state EXCEPT ![request.dest] = "fenced"]
        /\ rep_committed_offset' = [rep_committed_offset EXCEPT ![request.dest] = 0]
        /\ RemoveAnyListeners(request.dest)
        /\ SendReply(request, [type   |-> "fencing_response",
                               epoch  |-> request.epoch,
                               source |-> request.dest,
                               ler    |-> rep_ler[request.dest]])
        /\ UNCHANGED << rep_leader, rep_log, rep_ler,
                        rep_first_offset, CoordVars,
                        confirmed, start_stop_ctr, confirmed_replicas, responses_processed >>

ReceivedLerResponseEnsemble ==
    { r \in DOMAIN coord_election_ler[coord_epoch] : coord_election_ler[coord_epoch][r].offset >= 0 }

\* The coordinator processes a fencing response and if it has receives a quorum of responses
\* it moves to the next phase of the election: choosing a leader
CoordinatorHandlesFencingResponse ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ NotProcessedResponse(response)
            /\ response.type = "fencing_response"
            /\ response.epoch = coord_epoch
            /\ coord_election[coord_epoch] = "fencing"
            /\ coord_election_ler[coord_epoch][response.source] = PendingFencingResponse
            /\ coord_state[response.source] # "offline"
            \* Actions
            /\ coord_election_ler' = [coord_election_ler EXCEPT ![coord_epoch] =
                                            [@ EXCEPT ![response.source] = response.ler]]
            /\ coord_state' = [coord_state EXCEPT ![response.source] = "fenced"]
            /\ IF IsQuorumMinusOne(ReceivedLerResponseEnsemble) THEN
                 coord_election' = [coord_election EXCEPT ![coord_epoch] = "notify_leader"]
               ELSE
                 coord_election' = coord_election
            /\ ResponseProcessed(response)
            /\ UNCHANGED << RepVars, coord_epoch, coord_leader, confirmed, confirmed_replicas,
                            start_stop_ctr, confirmed_replicas, requests >>

\* The coordinator selects the replica with the highest LER and sends it
\* a become leader request
CoordinatorSendsBecomeLeaderRequest ==
    /\ coord_epoch > 0
    /\ coord_election[coord_epoch] = "notify_leader"
    /\ LET candidates == CoordinatorVisibleReplicas
       IN
           /\ IsQuorum(candidates)
           /\ LET new_leader == CHOOSE cand \in candidates :
                                    \A rep \in candidates :
                                        CompareRecords(coord_election_ler[coord_epoch][cand], coord_election_ler[coord_epoch][rep]) >= 0
              IN
                   /\ coord_election' = [coord_election EXCEPT ![coord_epoch] = "pending_leader_confirm"]
                   /\ SendRequest([type  |-> "become_leader_request",
                                   epoch |-> coord_epoch,
                                   dest  |-> new_leader])
    /\ UNCHANGED << RepVars, coord_state, coord_epoch, coord_leader, coord_election_ler,
                    confirmed, start_stop_ctr, confirmed_replicas, responses_processed >>

\* A replica processes a become leader request and responds with a confirmation reply
ReplicaHandlesBecomeLeaderRequest ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "become_leader_request"
        /\ rep_state[request.dest] = "fenced"
        /\ rep_epoch[request.dest] = request.epoch
        \* Actions
        /\ rep_state' = [rep_state EXCEPT ![request.dest] = "leader"]
        /\ rep_leader' = [rep_leader EXCEPT ![request.dest] = request.dest]
        /\ SendReply(request, [type   |-> "become_leader_response",
                               epoch  |-> request.epoch,
                               source |-> request.dest])
        /\ UNCHANGED << rep_log, rep_ler, rep_epoch, rep_max_ler_of_rep, rep_listener,
                        rep_committed_offset, rep_first_offset, CoordVars, confirmed,
                        start_stop_ctr, confirmed_replicas, responses_processed >>

\* The coordinator process the become leader response and moves to the next
\* election phase: leader_elected where followers are notified
CoordinatorHandlesBecomeLeaderResponse ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ NotProcessedResponse(response)
            /\ response.type = "become_leader_response"
            /\ response.epoch = coord_epoch
            /\ coord_state[response.source] = "fenced"
            /\ coord_election[coord_epoch] = "pending_leader_confirm"
            \* Actions
            /\ coord_state' = [coord_state EXCEPT ![response.source] = "leader"]
            /\ coord_leader' = response.source
            /\ coord_election' = [coord_election EXCEPT ![coord_epoch] = "leader_elected"]
            /\ ResponseProcessed(response)
            /\ UNCHANGED << RepVars, coord_epoch, coord_election_ler, confirmed, confirmed_replicas, start_stop_ctr, requests >>

\* The coordinator sends a become follower request to a replica
\* that is not the new leader
CoordinatorSendsBecomeFollowerRequest ==
    /\ coord_epoch > 0
    /\ coord_election[coord_epoch] = "leader_elected"
    /\ \E r \in CoordinatorVisibleReplicas :
        /\ r # coord_leader
        \* should we restrict to only fenced replicas? What if a replica came back online during election process?
        /\ SendRequest([type      |-> "become_follower_request",
                        epoch     |-> coord_epoch,
                        leader    |-> coord_leader,
                        dest      |-> r])
    /\ UNCHANGED << RepVars, CoordVars, confirmed, confirmed_replicas, start_stop_ctr, responses_processed >>

\* A replica processes a become follower request and
\* - if it has an empty log it immediately becomes a follower
\* - else it enters the pending truncate state
ReplicaHandlesBecomeFollowerRequest ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ request.type = "become_follower_request"
        /\ rep_state[request.dest] = "fenced"
        /\ rep_epoch[request.dest] = request.epoch
        \* Actions
        /\ \/ /\ rep_log[request.dest] = {}
              /\ rep_state' = [rep_state EXCEPT ![request.dest] = "follower"]
           \/ /\ rep_log[request.dest] /= {}
              /\ rep_state' = [rep_state EXCEPT ![request.dest] = "pending_truncate"]
        /\ rep_leader' = [rep_leader EXCEPT ![request.dest] = request.leader]
        /\ UNCHANGED << rep_log, rep_ler, rep_epoch, rep_max_ler_of_rep, rep_listener,
                        rep_committed_offset, rep_first_offset, CoordVars, confirmed,
                        start_stop_ctr, requests, confirmed_replicas, responses_processed >>

\* -----------------------------------------
\* LOG TRUNCATION AFTER ELECTION or START
\* -----------------------------------------

\* Get the highest epoch in this replicas log
GetMaxEpochInLog(r) ==
    LET rec == CHOOSE record \in rep_log[r] :
                    \A rec \in rep_log[r] : record.epoch >= rec.epoch
    IN rec.epoch

\* Return the highest record in this log that has the same or lower epoch
GetMaxRecordInLogUnderEpoch(r, epoch) ==
    LET potentials == { rcd \in rep_log[r] : rcd.epoch <= epoch }
    IN
        IF potentials # {} THEN
            CHOOSE record \in potentials : \A rec \in potentials : CompareRecords(record, rec) >= 0
        ELSE NoMetaRecord

\* Return the highest record in this log that either equal to or less than the given record
GetMaxRecordInLogEqLowerToRecord(r, record) ==
    IF \E rcd \in rep_log[r] : rcd.epoch = record.epoch /\ rcd.offset = record.offset THEN
        CHOOSE rcd \in rep_log[r] : rcd.epoch = record.epoch /\ rcd.offset = record.offset
    ELSE
        LET potentials == { rcd \in rep_log[r] : CompareRecords(rcd, record) = -1 }
        IN
            IF potentials # {} THEN
                CHOOSE rec1 \in potentials : \A rec2 \in potentials : CompareRecords(rec1, rec2) >= 0
            ELSE
                NoMetaRecord

\* Is this offset, in this replica, quorum written - 1 (according to its own bookkeeping)
QuorumWrittenMinusOne(offset, leader) ==
    Cardinality({ r \in R : rep_max_ler_of_rep[leader][r].offset >= offset})
                >= (Cardinality(R) \div 2)

\* Is this offset, in this replica, quorum written (according to its own bookkeeping)
QuorumWritten(record, leader) ==
    Cardinality({ r \in R : rep_max_ler_of_rep[leader][r].offset >= record.offset})
                >= (Cardinality(R) \div 2) + 1

\* Return a new log that is truncated to this record
TruncateLogTo(log, record) ==
    { rec \in log : CompareRecords(rec, record) <= 0 }

\* Return a new ler that is the lower of the existing one and the provided one
TruncateLerTo(r, record) ==
    IF CompareRecords(record, rep_ler[r]) = -1 THEN
        [offset |-> record.offset, epoch |-> record.epoch]
    ELSE rep_ler[r]

\* Truncate the log and ler to this record, which has bee identified as the highest
\* safe record where no divergence has occurred
TruncateToSafePoint(r, record) ==
\* Uncomment this to trigger divergence
\*    /\ rep_log' = rep_log
\*    /\ rep_ler' = rep_ler
    LET max_safe_record == GetMaxRecordInLogEqLowerToRecord(r, record)
    IN
        /\ rep_log' = [rep_log EXCEPT ![r] =
                        TruncateLogTo(rep_log[r], max_safe_record)]
        /\ rep_ler' = [rep_ler EXCEPT ![r] = TruncateLerTo(r, max_safe_record)]

\* A replica in the pending truncate state potentially needs to truncate their log
\* to match the new leader. So it sends a ler_at_epoch_request to the new leader,
\* including the highest epoch it has in its log.
ReplicaSendsLastOffsetAtEpochRequest ==
    \E r \in R :
        /\ rep_leader[r] # 0
        /\ rep_state[r] = "pending_truncate"
        /\ LET last_epoch == GetMaxEpochInLog(r)
           IN SendRequest([type          |-> "ler_at_epoch_request",
                           epoch         |-> rep_epoch[r],
                           target_epoch  |-> last_epoch,
                           dest          |-> rep_leader[r],
                           source        |-> r])
        /\ UNCHANGED << RepVars, CoordVars, confirmed, confirmed_replicas, start_stop_ctr, responses_processed >>

\* The leader replica processes the ler_at_epoch_request and responds with
\* the highest record it has in its log, at that particular epoch or lower.
\* Note that the current election epoch may not match this target epoch, especially
\* if the requesting replica has been offline for some time
LeaderReplicaHandlesLastOffsetAtEpochRequest ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "ler_at_epoch_request"
        /\ rep_state[request.dest] = "leader"
        /\ rep_epoch[request.dest] = request.epoch
        \* Actions
        /\ LET last_record == GetMaxRecordInLogUnderEpoch(request.dest, request.target_epoch)
           IN SendReply(request, [type          |-> "ler_at_epoch_response",
                                  epoch         |-> rep_epoch[request.dest],
                                  record        |-> last_record,
                                  target_epoch  |-> request.target_epoch,
                                  dest          |-> request.source,
                                  source        |-> request.dest])
        /\ UNCHANGED << RepVars, CoordVars, confirmed, confirmed_replicas, start_stop_ctr, responses_processed >>


\* A replica in the pending_truncate state receives a response with the highest
\* record the leader has at the requested epoch or lower
\* The replica then truncates its log to remove records with higher epochs/offsets than
\* those of the leader. This avoids log divergence.
\* The replica now transitions to the follower state where it can begin replication.
ReplicaHandlesLastOffsetAtEpochResponse ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ NotProcessedResponse(response)
            /\ response.type = "ler_at_epoch_response"
            /\ rep_state[response.dest] = "pending_truncate"
            /\ rep_epoch[response.dest] = response.epoch
            \* Actions
            /\ TruncateToSafePoint(response.dest, response.record)
            /\ rep_state' = [rep_state EXCEPT ![response.dest] = "follower"]
            /\ ResponseProcessed(response)
            /\ UNCHANGED << rep_leader, rep_epoch, rep_max_ler_of_rep, rep_listener,
                            rep_committed_offset, rep_first_offset, CoordVars, confirmed,
                            start_stop_ctr, confirmed_replicas, requests >>

\* ------------------------------------------
\* WRITES and REPLICATION
\* ------------------------------------------

\* A client writes a value from V to a leader replica, if that
\* value has not previously been written
\* The leader adds the record to its log, updates its LER
Write ==
    \E r \in R, v \in V :
        \* enabling conditions
        /\ rep_state[r] = "leader"
        /\ v \notin DOMAIN confirmed
        \* actions
        /\ LET record == [offset |-> rep_ler[r].offset + 1,
                          epoch  |-> rep_epoch[r],
                          value  |-> v]
               meta_record == [offset |-> rep_ler[r].offset + 1,
                               epoch  |-> rep_epoch[r]]
           IN
                /\ rep_log' = [rep_log EXCEPT ![r] = @ \union { record }]
                /\ rep_ler' = [rep_ler EXCEPT ![r] = meta_record]
                /\ rep_max_ler_of_rep' = [rep_max_ler_of_rep EXCEPT ![r] = [@ EXCEPT ![r] = meta_record]]
                /\ confirmed' = confirmed @@ (v :> -1)
                /\ confirmed_replicas' = confirmed_replicas @@ (v :> {})
        /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_listener,
                        rep_committed_offset, rep_first_offset, CoordVars, start_stop_ctr,
                        requests, responses_processed >>

\* A follower sends a start_listener_request to the replica it believes to be the leader
\* It includes its current LER
ReplicaRequestsDataListener ==
    \E r \in R :
        /\ rep_state[r] = "follower"
        /\ rep_leader[r] # 0
        /\ rep_listener[rep_leader[r]][r] = FALSE
        /\ SendRequest([type   |-> "start_listener_request",
                        epoch  |-> rep_epoch[r],
                        ler    |-> rep_ler[r],
                        dest   |-> rep_leader[r],
                        source |-> r])
        /\ UNCHANGED << RepVars, CoordVars, confirmed, confirmed_replicas, start_stop_ctr, responses_processed >>

\* The leader registers a data listener, with the current position of the follower
ReplicaLeaderRegistersDataListener ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "start_listener_request"
        /\ rep_state[request.dest] = "leader"
        /\ rep_epoch[request.dest] = request.epoch
        /\ rep_listener[request.dest][request.source] = FALSE
        \* actions
        /\ rep_max_ler_of_rep' = [rep_max_ler_of_rep EXCEPT ![request.dest] =
                                [@ EXCEPT ![request.source] = request.ler]]
        /\ rep_listener' = [rep_listener EXCEPT ![request.dest] =
                                [@ EXCEPT ![request.source] = TRUE]]
        /\ SendReply(request, [type |-> "start_listener_response"])
        /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_log, rep_ler,
                        rep_committed_offset, rep_first_offset, CoordVars, confirmed,
                        start_stop_ctr, confirmed_replicas, responses_processed >>

\* Get the next lowest record above the current replica position
NextRecord(leader, last_record) ==
    LET unreplicated == { rec \in rep_log[leader] : CompareRecords(rec, last_record) = 1 }
    IN CHOOSE next_record \in unreplicated : \A rd \in unreplicated : next_record.offset <= rd.offset

\* The replica leader will send records when it has records in its log that are higher
\* than the current replica position
ReplicaLeaderSendsData ==
    \E leader \in R :
        \E follower \in R :
            \* Conditions to process the message
            /\ rep_state[leader] = "leader"
            /\ rep_listener[leader][follower] = TRUE
            /\ CompareRecords(rep_max_ler_of_rep[leader][follower], rep_ler[leader]) = -1
            \* Actions
            /\ LET next == NextRecord(leader, rep_max_ler_of_rep[leader][follower])
               IN
                    SendRequest([type      |-> "send_data_request",
                                 epoch     |-> rep_epoch[leader],
                                 record    |-> next,
                                 dest      |-> follower,
                                 source    |-> leader])
            /\ UNCHANGED << RepVars, CoordVars, confirmed, confirmed_replicas, start_stop_ctr, responses_processed >>

\* A replica sends an acknowledgement of records from the leader replica
ReplicaAcksData ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "send_data_request"
        /\ rep_state[request.dest] = "follower"
        /\ rep_epoch[request.dest] = request.epoch
        \* actions
        /\ LET meta_rec == [offset |-> request.record.offset,
                            epoch  |-> request.record.epoch]
               new_ler == IF request.record.offset > rep_ler[request.dest].offset
                          THEN meta_rec
                          ELSE rep_ler[request.dest]
           IN
                /\ rep_log' = [rep_log EXCEPT ![request.dest] = @ \union { request.record }]
                /\ rep_ler' = [rep_ler EXCEPT ![request.dest] = new_ler]
                /\ SendReply(request, [type          |-> "send_data_response",
                                       epoch         |-> rep_epoch[request.dest],
                                       record        |-> meta_rec,
                                       dest          |-> request.source,
                                       source        |-> request.dest])
        /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_max_ler_of_rep, rep_listener,
                        rep_committed_offset, rep_first_offset, CoordVars, confirmed,
                        start_stop_ctr, confirmed_replicas, responses_processed >>

\* The leader replica processes an ack and updates the position of that replica
LeaderReplicaUpdatesReplicaPosition ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ NotProcessedResponse(response)
            /\ response.type = "send_data_response"
            /\ rep_state[response.dest] = "leader"
            /\ rep_epoch[response.dest] = response.epoch
            \* Actions
            /\ rep_max_ler_of_rep' = [rep_max_ler_of_rep EXCEPT ![response.dest] =
                                        [@ EXCEPT ![response.source] = response.record]]
            /\ ResponseProcessed(response)
            /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_log, rep_ler, rep_listener,
                            rep_committed_offset, rep_first_offset, CoordVars, confirmed,
                            start_stop_ctr, confirmed_replicas, requests >>

\* A leader replica will confirm a write if a quorum of replicas have confirmed
\* they have it (by including their LER in their fetch requests)
ConfirmWrite ==
    \E r \in R :
        /\ rep_state[r] = "leader"
        /\ \E record \in rep_log[r] :
            /\ confirmed[record.value] = -1
            /\ QuorumWritten(record, r)
            /\ confirmed' = [confirmed EXCEPT ![record.value] = record.offset]
            /\ confirmed_replicas' = confirmed_replicas @@
                (record.value :> { rr \in R : rep_max_ler_of_rep[r][rr].offset >= record.offset })
        /\ UNCHANGED << RepVars, CoordVars, start_stop_ctr, requests, responses_processed >>

\* The leader computes its committed offset from quorum acknowledgements
\* and updates its own view. This happens implicitly as acks arrive.
LeaderAdvancesCommittedOffset ==
    \E r \in R :
        /\ rep_state[r] = "leader"
        /\ LET new_committed == CommittedOffset(r)
           IN /\ new_committed > rep_committed_offset[r]
              /\ rep_committed_offset' = [rep_committed_offset EXCEPT ![r] = new_committed]
        /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_log, rep_ler,
                        rep_max_ler_of_rep, rep_listener, rep_first_offset,
                        CoordVars, confirmed, start_stop_ctr, requests, confirmed_replicas, responses_processed >>

\* The leader propagates its committed offset to a follower via the
\* replication channel.
LeaderSendsCommittedOffset ==
    \E leader \in R :
        \E follower \in R :
            /\ rep_state[leader] = "leader"
            /\ rep_listener[leader][follower] = TRUE
            /\ rep_committed_offset[leader] > rep_committed_offset[follower]
            /\ SendRequest([type             |-> "committed_offset_request",
                            epoch            |-> rep_epoch[leader],
                            committed_offset |-> rep_committed_offset[leader],
                            dest             |-> follower,
                            source           |-> leader])
        /\ UNCHANGED << RepVars, CoordVars, confirmed, confirmed_replicas, start_stop_ctr, responses_processed >>

\* A follower receives the committed offset from the leader
ReplicaHandlesCommittedOffset ==
    \E request \in DOMAIN requests :
        /\ NotSentReply(request)
        /\ request.type = "committed_offset_request"
        /\ rep_state[request.dest] = "follower"
        /\ rep_epoch[request.dest] = request.epoch
        \* Only advance, never go backwards
        /\ request.committed_offset > rep_committed_offset[request.dest]
        /\ rep_committed_offset' = [rep_committed_offset EXCEPT
                                    ![request.dest] = request.committed_offset]
        /\ SendReply(request, [type |-> "committed_offset_response"])
        /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_log, rep_ler,
                        rep_max_ler_of_rep, rep_listener, rep_first_offset,
                        CoordVars, confirmed, start_stop_ctr, confirmed_replicas, responses_processed >>

\* ------------------------------------------
\* RETENTION
\* ------------------------------------------

\* A replica deletes a prefix of its log. This models local retention
\* (max-bytes, max-age). Any replica can retain independently.
\* NOTE: this means confirmed writes can be lost if the leader retains
\* data and then an election causes the replicas that had it to truncate
\* or reset. This is a known property of osiris (performance over durability).
RetentionDeletesPrefix ==
    \E r \in R :
        /\ rep_state[r] \in {"leader", "follower"}
        /\ rep_log[r] # {}
        /\ \E record \in rep_log[r] :
            \* Must leave at least one record
            /\ \E remaining \in rep_log[r] : remaining.offset > record.offset
            \* Actions: delete all records at or below this offset
            /\ rep_log' = [rep_log EXCEPT ![r] =
                            { rec \in @ : rec.offset > record.offset }]
            /\ rep_first_offset' = [rep_first_offset EXCEPT ![r] = record.offset + 1]
            /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_ler,
                            rep_max_ler_of_rep, rep_listener, rep_committed_offset,
                            CoordVars, confirmed, start_stop_ctr, requests,
                            confirmed_replicas, responses_processed >>

\* A replica that falls behind retention on the leader must resync.
\* When the leader tries to send data but the follower needs an offset
\* that has been retained, the follower resets and starts from the
\* leader's current first offset.
\* In the model: if a follower's LER is below the leader's first_offset,
\* the follower resets its log to empty and re-registers.
ReplicaFallsBehindRetention ==
    \E leader \in R :
        \E follower \in R :
            /\ rep_state[leader] = "leader"
            /\ rep_state[follower] = "follower"
            /\ rep_epoch[follower] = rep_epoch[leader]
            /\ rep_listener[leader][follower] = TRUE
            /\ rep_first_offset[leader] > 0
            /\ rep_ler[follower].offset < rep_first_offset[leader]
            \* The leader must be coord-visible to communicate with followers
            /\ coord_state[leader] # "offline"
            \* Actions: follower resets
            /\ rep_log' = [rep_log EXCEPT ![follower] = {}]
            /\ rep_ler' = [rep_ler EXCEPT ![follower] = NoMetaRecord]
            /\ rep_first_offset' = [rep_first_offset EXCEPT ![follower] = rep_first_offset[leader]]
            /\ rep_committed_offset' = [rep_committed_offset EXCEPT ![follower] = 0]
            /\ rep_listener' = [rep_listener EXCEPT ![leader] =
                                    [@ EXCEPT ![follower] = FALSE]]
            /\ rep_max_ler_of_rep' = [rep_max_ler_of_rep EXCEPT ![leader] =
                                    [@ EXCEPT ![follower] = NoMetaRecord]]
            /\ UNCHANGED << rep_state, rep_epoch, rep_leader,
                            CoordVars, confirmed, start_stop_ctr, requests,
                            confirmed_replicas, responses_processed >>


Next ==
    \/ ReplicaStarts
    \/ ReplicaStops
    \* Election
    \/ CoordinatorTriggersElection              \* election starts
    \/ CoordinatorSendsFencingRequest           \* fencing phase
    \/ ReplicaHandlesFencingRequest             \* fencing phase
    \/ CoordinatorHandlesFencingResponse        \* fencing phase
    \/ CoordinatorSendsBecomeLeaderRequest      \* choose leader phase
    \/ ReplicaHandlesBecomeLeaderRequest        \* choose leader phase
    \/ CoordinatorHandlesBecomeLeaderResponse   \* choose leader phase
    \/ CoordinatorSendsBecomeFollowerRequest    \* notify followers phase
    \/ ReplicaHandlesBecomeFollowerRequest      \* notify followers phase
    \* Truncate log on becoming follower
    \/ ReplicaSendsLastOffsetAtEpochRequest
    \/ LeaderReplicaHandlesLastOffsetAtEpochRequest
    \/ ReplicaHandlesLastOffsetAtEpochResponse
    \* Visibility
    \/ CoordinatorLosesVisibilityOfReplica
    \/ CoordinatorRestoresVisibilityOfReplica
    \* Writes and replication
    \/ Write
    \/ ReplicaRequestsDataListener
    \/ ReplicaLeaderRegistersDataListener
    \/ ReplicaLeaderSendsData
    \/ ReplicaAcksData
    \/ ConfirmWrite
    \/ LeaderAdvancesCommittedOffset
    \/ LeaderSendsCommittedOffset
    \/ ReplicaHandlesCommittedOffset
    \* Retention
    \/ RetentionDeletesPrefix
    \/ ReplicaFallsBehindRetention

\*-------------------------------------------------
\*INVARIANTS
\*-------------------------------------------------

RepState == { "offline", "fenced", "pending_truncate", "follower", "leader" }
ReplicaOrNone == { 0 } \union R
RepAckedValues == {PendingFencingRequest, PendingFencingResponse } \union Nat

\* Note that we do not use an "election complete" phase to reduce state space, the last phase is leader_elected
ElectionPhases == { "fencing", "notify_leader", "pending_leader_confirm", "leader_elected", "aborted" }

TypeOK ==
    /\ rep_state \in [R -> RepState]
    /\ rep_leader \in [R -> ReplicaOrNone]
    /\ rep_log \in [R -> SUBSET Record]
    /\ rep_ler \in [R -> MetaRecord]
    /\ rep_epoch \in [R -> Nat]
    /\ rep_max_ler_of_rep \in [R -> [R -> MetaRecord]]
    /\ rep_listener \in [R -> [R -> BOOLEAN]]
    /\ rep_committed_offset \in [R -> Nat]
    /\ rep_first_offset \in [R -> Nat]
    /\ coord_state \in [R -> RepState]
    /\ coord_epoch \in Nat
    /\ coord_leader \in ReplicaOrNone
    /\ \A epoch \in DOMAIN coord_election : coord_election[epoch] \in ElectionPhases
    /\ start_stop_ctr \in Nat

\* There cannot exist a record in a non-stale follower that CONFLICTS with
\* the leader's log. A record conflicts if the leader has a DIFFERENT record
\* at the same offset. Records the leader doesn't have (due to retention
\* gaps) are not conflicts — they're stale but harmless.
NoDivergence ==
    \/ coord_leader = 0
    \/ /\ coord_leader # 0
       /\ \A r \in R :
            IF rep_state[r] = "follower" /\ rep_epoch[r] = rep_epoch[coord_leader] THEN
                \A record \in rep_log[r] :
                    \/ record \in rep_log[coord_leader]
                    \/ record.offset < rep_first_offset[coord_leader]
                    \/ ~\E other \in rep_log[coord_leader] :
                        /\ other.offset = record.offset
                        /\ other # record
            ELSE
                TRUE

FollowerEqualOrLowerEpoch ==
     \/ coord_leader = 0
     \/ /\ coord_leader # 0
        /\ \A r \in R :
            \/ rep_state[r] # "follower"
            \/ /\ rep_state[r] = "follower"
               /\ rep_epoch[r] <= rep_epoch[coord_leader]

\* A confirmed write must exist on at least one replica's log, UNLESS
\* every replica that had it at confirmation time has since retained past it.
\* This catches bugs where data disappears without retention being the cause.
\* confirmed[v] = -1 means unconfirmed; confirmed[v] >= 0 means confirmed at that offset.
NoLossOfConfirmedWrite ==
    IF coord_leader # 0 /\ Cardinality(DOMAIN confirmed) > 0 THEN
        \A v \in DOMAIN confirmed :
            IF confirmed[v] = -1 THEN TRUE
            ELSE
                \/ \E r \in R : \E record \in rep_log[r] : record.value = v
                \/ \A r \in confirmed_replicas[v] :
                    confirmed[v] < rep_first_offset[r]
    ELSE
        TRUE

LerMatchesLog ==
    \A r \in R :
        IF rep_log[r] = {} THEN
            TRUE
        ELSE
            LET last_record == CHOOSE record \in rep_log[r] :
                                \A rec \in rep_log[r] : record.offset >= rec.offset
            IN rep_ler[r].offset = last_record.offset


\* A follower's committed offset never exceeds the leader's committed offset
\* (it may lag behind due to propagation delay)
FollowerCommittedOffsetBounded ==
    \/ coord_leader = 0
    \/ /\ coord_leader # 0
       /\ \A r \in R :
            \/ rep_state[r] # "follower"
            \/ rep_epoch[r] # rep_epoch[coord_leader]
            \/ rep_committed_offset[r] <= rep_committed_offset[coord_leader]

TestInv ==
    \* Add debugging expressions here to trigger error trace
    TRUE


=============================================================================
\* Modification History
\* Last modified Wed May 13 18:08:24 CEST 2020 by jack
\* Created Mon Jan 27 18:48:47 CET 2020 by jack
