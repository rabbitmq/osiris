-------------------------- MODULE OsirisMsgPassing --------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS R, \* The set of all replicas
          V \* The set of all message payloads that can be sent

VARIABLES rep_state,            \* the state of each replica
          rep_epoch,            \* the epoch of each replica
          rep_leader,           \* the replica each replica believes is leader
          rep_log,              \* the log of each replica
          rep_leo,              \* the Log End Offset of each replica
          rep_max_leo_of_rep,   \* for each replica, what it knows to be the LEO of every other replica
          coord_state,          \* the state of each replica, according to the coordinator
          coord_epoch,          \* the current epoch
          coord_leader,         \* the replica that was elected leader
          coord_election,       \* the election phase of each epoch
          coord_election_leo,   \* the LEO of each fenced replica, per epoch
          confirmed,            \* the confirmation status of each written value
          start_stop_ctr,       \* a counter to limit the state space
          requests              \* the set of all requests and their responses

AllVars == << rep_state, rep_epoch, rep_leader, rep_log, rep_leo, rep_max_leo_of_rep, coord_state, coord_epoch,
          coord_leader, coord_election, coord_election_leo, confirmed,
          start_stop_ctr, requests >>
RepVars == << rep_state, rep_epoch, rep_leader, rep_log, rep_leo, rep_max_leo_of_rep >>
CoordVars == << coord_state, coord_epoch, coord_leader, coord_election, coord_election_leo >>
          
Init ==
    /\ rep_state = [r \in R |-> "follower"]
    /\ rep_epoch = [r \in R |-> 0]
    /\ rep_leader = [r \in R |-> 0]
    /\ rep_log = [r \in R |-> {}]
    /\ rep_leo = [r \in R |-> 0]
    /\ rep_max_leo_of_rep = [r \in R |-> [r2 \in R |-> 0]]
    /\ coord_state = [r \in R |-> "follower"]
    /\ coord_epoch = 0
    /\ coord_leader = 0
    /\ coord_election = [e \in {} |-> 0]
    /\ coord_election_leo = [e \in {} |-> 0]
    /\ confirmed = [w \in {} |-> 0]
    /\ start_stop_ctr = 0
    /\ requests = [req \in {} |-> 0]

\* -----------------------------------------
\* HELPER OPERATORS
\* ----------------------------------------- 

PendingFencingRequest == -2
PendingFencingResponse == -1

\* Send a request only if the request has already not been sent
SendRequest(request) ==
    /\ request \notin DOMAIN requests
    /\ requests' = requests @@ (request :> [type |-> "-"])
    
\* Send a reply to a request, given the request has been sent
SendReply(request, reply) ==
    /\ request \in DOMAIN requests
    /\ requests' = [requests EXCEPT ![request] = reply]

\* A reply was sent or not?
NotSentReply(request) ==
    /\ request \in DOMAIN requests
    /\ requests[request].type = "-"

\* The replicas visible to the coordinator    
CoordinatorVisibleReplicas ==
    { r \in R : coord_state[r] # "offline" }    

\* Does this set of replicas consistitute a quorum?    
IsQuorum(replicas) ==
    Cardinality(replicas) >= (Cardinality(R) \div 2) + 1    

\* Does this set of replicas consistitute one short of a quorum?    
IsQuorumMinusOne(replicas) ==
    Cardinality(replicas) >= (Cardinality(R) \div 2)

\* -----------------------------------------
\* REPLICA STAR/STOP AND COORD-REPLICA VISIBILITY
\* ----------------------------------------- 

\* The coordinator tells the replica the current epoch, who the current leader is (if any)
\* A replica that is started can only become a follower if it has an empty log
\* Else it must enter the pending_truncate state 
CoordinatorResetsReplica(r) ==
    LET new_state == IF rep_leo[r] = 0 THEN "follower" ELSE "pending_truncate"
    IN
        /\ rep_state' = [rep_state EXCEPT ![r] = new_state]
        /\ coord_state' = [coord_state EXCEPT ![r] = new_state]
        /\ rep_epoch' = [rep_epoch EXCEPT ![r] = coord_epoch]
        /\ rep_leader' = [rep_leader EXCEPT ![r] = coord_leader]

\* An offline replica starts up and is reset
ReplicaStarts ==
    /\ \E r \in R :
        /\ rep_state[r] = "offline"
        /\ coord_state[r] = "offline"
        \* Extra restriction to reduce state space by not allowing stopping during elections: start
        /\ coord_election[coord_epoch] = "notify_followers"
        \* end
        /\ CoordinatorResetsReplica(r)
        /\ start_stop_ctr' = start_stop_ctr + 1
        /\ UNCHANGED << rep_log, rep_leo, rep_max_leo_of_rep, coord_epoch, coord_leader, coord_election, 
                        coord_election_leo, confirmed, requests >>

\* An online replica stops
\* The coordinator is immediately notified. The stopped replica could be the leader.
ReplicaStops ==
    /\ \E r \in R :
        /\ rep_state[r] # "offline"
        \* Extra restriction to reduce state space by not allowing stopping during elections: start
        /\ coord_epoch # 0
        /\ coord_election[coord_epoch] = "notify_followers"
        \* end
        /\ rep_state' = [rep_state EXCEPT ![r] = "offline"]
        /\ coord_state' = [coord_state EXCEPT ![r] = "offline"]
        /\ \/ /\ coord_leader = r
              /\ coord_leader' = 0
           \/ /\ coord_leader # r
              /\ coord_leader' = coord_leader
    /\ start_stop_ctr' = start_stop_ctr + 1
    /\ UNCHANGED << rep_epoch, rep_leader, rep_log, rep_leo, rep_max_leo_of_rep,  
                    coord_epoch, coord_election, coord_election_leo,
                    confirmed, requests >> 

\* A network partition causes the coordinator to lose visibility of a replica
\* The replica could be the current leader
CoordinatorLosesVisibilityOfReplica ==
    /\ \E r \in R :
        /\ rep_state[r] # "offline"
        /\ coord_state[r] # "offline"
        \* Extra restriction to reduce state space by not allowing vis loss during elections: start
        /\ coord_epoch # 0
        /\ coord_election[coord_epoch] = "notify_followers"
        \* end
        /\ coord_state' = [coord_state EXCEPT ![r] = "offline"]
        /\ \/ /\ r = coord_leader
              /\ coord_leader' = 0
           \/ /\ r # coord_leader
              /\ coord_leader' = coord_leader
        /\ start_stop_ctr' = start_stop_ctr + 1
    /\ UNCHANGED << RepVars, coord_epoch, coord_election, coord_election_leo, confirmed, requests >>

\* The network partition between the coordinator and the replica is resolved
\* The coordinator tells the replica to reset itself
CoordinatorRestoresVisibilityOfReplica ==
    /\ \E r \in R :
        /\ rep_state[r] # "offline"
        /\ coord_state[r] = "offline"
        /\ CoordinatorResetsReplica(r)
        /\ start_stop_ctr' = start_stop_ctr + 1
    /\ UNCHANGED << rep_log, rep_leo, rep_max_leo_of_rep, coord_leader,
                    coord_epoch, coord_election, coord_election_leo, confirmed, requests >>
        
\* ------------------------------------------
\* LEADER ELECTION
\* ------------------------------------------

\* The coordinator sees there is no current leader
\* Having no leader triggers an election if an election process is not ingoing or
\* it has reached the last phase.
\* A replica chosen to be leader that dies before responding is not currently modelled and is
\* a liveness property
CoordinatorTriggersElection ==
    \* Trigger election when there is no leader and election phase not pre notify followers
    /\ coord_leader = 0
    /\ \/ coord_epoch = 0
       \/ /\ coord_epoch > 0
          /\ coord_election[coord_epoch] = "notify_followers"
    /\ IsQuorum(CoordinatorVisibleReplicas)
    \* Actions
    /\ LET new_epoch == coord_epoch + 1
       IN  /\ coord_epoch' = new_epoch
           /\ coord_election' = coord_election @@ (new_epoch :> "fencing")
           /\ coord_election_leo' = coord_election_leo @@ (new_epoch :> [r \in R |-> PendingFencingRequest])
           /\ UNCHANGED << RepVars, coord_state, coord_leader, confirmed, start_stop_ctr, requests >>
           
\* The first phase of leader election is to fence a quorum of replicas
\* A fenced replica will stop any replication with a stale leader
\* The coordinator sends a replica a fencing request
CoordinatorSendsFencingRequest ==
    /\ coord_epoch > 0
    /\ coord_election[coord_epoch] = "fencing"
    /\ \E r \in R :
        /\ coord_state[r] # "offline"
        /\ coord_election_leo[coord_epoch][r] = PendingFencingRequest
        /\ coord_election_leo' = [coord_election_leo EXCEPT ![coord_epoch] =
                                    [@ EXCEPT ![r] = PendingFencingResponse]]
        /\ SendRequest([type   |-> "fencing_request",
                        epoch  |-> coord_epoch,
                        dest   |-> r])
    /\ UNCHANGED << RepVars, coord_state, coord_epoch, coord_leader, coord_election,
                    confirmed, start_stop_ctr >>

\* A replica processes a fencing requests, fences itself and responds with its LEO                    
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
        /\ SendReply(request, [type   |-> "fencing_response",
                               epoch  |-> request.epoch,
                               source |-> request.dest,
                               leo    |-> rep_leo[request.dest]])
        /\ UNCHANGED << rep_leader, rep_log, rep_leo, rep_max_leo_of_rep, CoordVars, confirmed,
                        start_stop_ctr >>

ReceivedLeoResponseEnsemble ==
    { r \in DOMAIN coord_election_leo[coord_epoch] : coord_election_leo[coord_epoch][r] >= 0 }

\* The coordinator processes a fencing response and if it has receives a quorum of responses
\* it moves to the next phase of the election: choosing a leader
CoordinatorHandlesFencingResponse ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ response.type = "fencing_response"
            /\ response.epoch = coord_epoch
            /\ coord_election[coord_epoch] = "fencing"
            /\ coord_election_leo[coord_epoch][response.source] = PendingFencingResponse
            /\ coord_state[response.source] # "offline"
            \* Actions 
            /\ coord_election_leo' = [coord_election_leo EXCEPT ![coord_epoch] =
                                            [@ EXCEPT ![response.source] = response.leo]]
            /\ coord_state' = [coord_state EXCEPT ![response.source] = "fenced"]                                            
            /\ IF IsQuorumMinusOne(ReceivedLeoResponseEnsemble) THEN
                 coord_election' = [coord_election EXCEPT ![coord_epoch] = "notify_leader"]
               ELSE
                 coord_election' = coord_election
            /\ UNCHANGED << RepVars, coord_epoch, coord_leader, confirmed, 
                            start_stop_ctr, requests >>
                            
\* The coordinator selects the replica with the highest LEO and sends it
\* a become leader request
CoordinatorSendsBecomeLeaderRequest ==
    /\ coord_epoch > 0
    /\ coord_election[coord_epoch] = "notify_leader"
    /\ LET candidates == CoordinatorVisibleReplicas
       IN
           /\ IsQuorum(candidates)
           /\ LET new_leader == CHOOSE cand \in candidates :
                                    \A rep \in candidates :
                                        coord_election_leo[coord_epoch][cand] >= coord_election_leo[coord_epoch][rep]
              IN
                   /\ coord_election' = [coord_election EXCEPT ![coord_epoch] = "pending_leader_confirm"]
                   /\ SendRequest([type  |-> "become_leader_request",
                                   epoch |-> coord_epoch,
                                   dest  |-> new_leader])
    /\ UNCHANGED << RepVars, coord_state, coord_epoch, coord_leader, coord_election_leo, 
                    confirmed, start_stop_ctr >>
                                
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
        /\ rep_max_leo_of_rep' = [rep_max_leo_of_rep EXCEPT ![request.dest] = [r \in R |-> 0]]
        /\ SendReply(request, [type   |-> "become_leader_response",
                               epoch  |-> request.epoch,
                               source |-> request.dest])
        /\ UNCHANGED << rep_log, rep_leo, rep_epoch, CoordVars, confirmed, start_stop_ctr >>

\* The coordinator process the become leader response and moves to the next
\* election phase: notify followers
CoordinatorHandlesBecomeLeaderResponse ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ response.type = "become_leader_response"
            /\ response.epoch = coord_epoch 
            /\ coord_state[response.source] = "fenced"
            /\ coord_election[coord_epoch] = "pending_leader_confirm"
            \* Actions
            /\ coord_state' = [coord_state EXCEPT ![response.source] = "leader"]
            /\ coord_leader' = response.source
            /\ coord_election' = [coord_election EXCEPT ![coord_epoch] = "notify_followers"]
            /\ UNCHANGED << RepVars, coord_epoch, coord_election_leo, confirmed, start_stop_ctr, requests >>

\* The coordinator sends a become follower request to a replica
\* that is not the new leader
CoordinatorSendsBecomeFollowerRequest ==
    /\ coord_epoch > 0
    /\ coord_election[coord_epoch] = "notify_followers"
    /\ \E r \in CoordinatorVisibleReplicas :
        /\ r # coord_leader
        \* should we restrict to only fenced replicas? What is a replica came back online during election process?
        /\ SendRequest([type   |-> "become_follower_request",
                        epoch  |-> coord_epoch,
                        leader |-> coord_leader,
                        dest   |-> r])
    /\ UNCHANGED << RepVars, CoordVars, confirmed, start_stop_ctr >>
                
\* A replica processes a become follower request and
\* - if it has an emoty log it immediately becomes a follower
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
        /\ rep_max_leo_of_rep' = [rep_max_leo_of_rep EXCEPT ![request.dest] = [r \in R |-> 0]]
        /\ UNCHANGED << rep_log, rep_leo, rep_epoch, CoordVars, confirmed, start_stop_ctr, requests >>

\* -----------------------------------------
\* LOG TRUNCATION AFTER ELECTION or START
\* ----------------------------------------- 

RecordsOfEpoch(r, epoch) ==
    { record \in rep_log[r] : record.epoch = epoch } 

GetMaxEpochInLog(r) ==
    LET rec == CHOOSE record \in rep_log[r] :
                    \A rec \in rep_log[r] : record.epoch >= rec.epoch
    IN rec.epoch

\* A replica in the pending truncate state potentially needs to truncate their log
\* to match the new leader. So it sends a leo_at_epoch_request to the new leader,
\* including the highest epoch it has in its log.
ReplicaSendsLastOffsetAtEpochRequest ==
    \E r \in R :
        /\ rep_leader[r] # 0
        /\ rep_state[r] = "pending_truncate" 
        /\ LET last_epoch == GetMaxEpochInLog(r)
           IN SendRequest([type          |-> "leo_at_epoch_request",
                           epoch         |-> rep_epoch[r],
                           target_epoch  |-> last_epoch,
                           dest          |-> rep_leader[r],
                           source        |-> r]) 
        /\ UNCHANGED << RepVars, CoordVars, confirmed, start_stop_ctr >>

\* The leader replica processes the leo_at_epoch_request and responds with
\* the highest offset it has in its log, at that particular epoch.
\* Note that the current election epoch may not match this target epoch, especially
\* if the requesting replica has been offline for some time
LeaderReplicaHandlesLastOffsetAtEpochRequest ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "leo_at_epoch_request"
        /\ rep_state[request.dest] = "leader"
        /\ rep_epoch[request.dest] = request.epoch
        \* Actions
        /\ LET records == RecordsOfEpoch(request.dest, request.target_epoch)
           IN 
                LET offset == IF \E rec \in records : \A rec2 \in records : rec.offset >= rec2.offset THEN
                                LET last == CHOOSE rec \in records : \A rec2 \in records : rec.offset >= rec2.offset
                                IN last.offset
                              ELSE 0
                IN SendReply(request, [type          |-> "leo_at_epoch_response",
                                       epoch         |-> rep_epoch[request.dest],
                                       offset        |-> offset,
                                       target_epoch  |-> request.target_epoch,
                                       dest          |-> request.source,
                                       source        |-> request.dest])
        /\ UNCHANGED << RepVars, CoordVars, confirmed, start_stop_ctr >>
    

TruncateLogTo(log, epoch, offset) ==
    { record \in log : record.epoch <= epoch /\ record.offset <= offset }

TruncateLeoTo(r, lastOffsetAtEpoch) ==
    IF lastOffsetAtEpoch < rep_leo[r] THEN
        lastOffsetAtEpoch
    ELSE rep_leo[r]
    
TruncateToSafePoint(response) ==
\* Uncomment this to trigger divergence
\*    /\ rep_log' = rep_log
\*    /\ rep_leo' = rep_leo
    /\ rep_log' = [rep_log EXCEPT ![response.dest] = 
                    TruncateLogTo(rep_log[response.dest],
                                  response.target_epoch,
                                  response.offset)]
    /\ rep_leo' = [rep_leo EXCEPT ![response.dest] = 
                        TruncateLeoTo(response.dest, response.offset)]       

\* A replica in the pending_truncate state receives a response with the highest
\* offset the leader has at the requested epoch
\* The replica then truncates its log to remove records with higher offsets than
\* those of the leader. This avoids log divergence.
\* The replica now transitions to the follower state where it can begin replication.
ReplicaHandlesLastOffsetAtEpochResponse ==
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ response.type = "leo_at_epoch_response"
            /\ rep_state[response.dest] = "pending_truncate"
            /\ rep_epoch[response.dest] = response.epoch
            \* Actions
            /\ TruncateToSafePoint(response)                                     
            /\ rep_state' = [rep_state EXCEPT ![response.dest] = "follower"]
            /\ UNCHANGED << rep_leader, rep_epoch, rep_max_leo_of_rep, CoordVars, confirmed, start_stop_ctr, requests >>

\* ------------------------------------------
\* WRITES and REPLICATION
\* ------------------------------------------

\* A client writes a value from V to a leader replica, if that
\* value has not previously been written
\* The leader adds the record to its log, updates its LEO       
Write ==
    \E r \in R, v \in V :
        \* enabling conditions 
        /\ rep_state[r] = "leader"
        /\ v \notin DOMAIN confirmed
        \* actions
        /\ LET record == [offset |-> rep_leo[r] + 1,
                          epoch |-> rep_epoch[r],
                          value |-> v]
           IN
                /\ rep_log' = [rep_log EXCEPT ![r] = @ \union { record }]
                /\ rep_leo' = [rep_leo EXCEPT ![r] = record.offset]
                /\ rep_max_leo_of_rep' = [rep_max_leo_of_rep EXCEPT ![r] = [@ EXCEPT ![r] = record.offset]]
                /\ confirmed' = confirmed @@ (v :> FALSE)
        /\ UNCHANGED << rep_state, rep_epoch, rep_leader, CoordVars, start_stop_ctr, requests >>

\* A follower sends a fetch request to the replica it believes to be the leader
\* It includes its current LEO
ReplicaSendsFetchRequest ==
    \E r \in R :
        /\ rep_state[r] = "follower"
        /\ rep_leader[r] # 0
        /\ SendRequest([type   |-> "fetch_request",
                        epoch  |-> rep_epoch[r],
                        leo    |-> rep_leo[r],
                        dest   |-> rep_leader[r],
                        source |-> r])
        /\ UNCHANGED << RepVars, CoordVars, confirmed, start_stop_ctr >>

NextRecord(leader, offset) ==
    LET unreplicated == { record \in rep_log[leader] : record.offset > offset }
    IN CHOOSE record \in unreplicated : \A rd \in unreplicated : record.offset <= rd.offset

\* A leader receives a fetch request and if it has records to replicate, it will reply
\* If the LEO of the follower is higher than the LEO it knows of for that replica, it will
\* update its cache of that
\* Note that in this spec, the leader does not reply to the follower if it has nothing to give it
\* this is an optimization to reduce the state space to avoid infinite request/response when there is not data
LeaderReplicaHandlesFetchRequest ==
    \E request \in DOMAIN requests :
        \* Conditions to process the message
        /\ NotSentReply(request)
        /\ request.type = "fetch_request"
        /\ rep_state[request.dest] = "leader" 
        /\ rep_epoch[request.dest] = request.epoch
        \* Actions
        /\ LET replicate       == rep_leo[request.dest] > request.leo
               advance_leo_pos == rep_max_leo_of_rep[request.dest][request.source] < request.leo
           IN
                /\ IF replicate THEN 
                        LET next == NextRecord(request.dest, request.leo)
                        IN SendReply(request, [type   |-> "fetch_response",
                                               epoch  |-> rep_epoch[request.dest],
                                               record |-> next,
                                               dest   |-> request.source,
                                               source |-> request.dest])
                   ELSE UNCHANGED << requests >>
                /\ IF advance_leo_pos THEN
                        rep_max_leo_of_rep' = [rep_max_leo_of_rep EXCEPT ![request.dest] = 
                                [@ EXCEPT ![request.source] = request.leo]]
                   ELSE UNCHANGED << rep_max_leo_of_rep >>
                /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_log, rep_leo, CoordVars, 
                                confirmed, start_stop_ctr >>

\* A follower replica processes the fetch response and adds any records to its log and updates its LEO
ReplicaHandlesFetchResponse == 
    \E req \in DOMAIN requests :
        LET response == requests[req]
        IN
            \* Conditions to process the message
            /\ response.type = "fetch_response"
            /\ rep_state[response.dest] = "follower" 
            /\ response.epoch = rep_epoch[response.dest] 
            /\ response.record.offset > rep_leo[response.dest]
            \* Actions
            /\ rep_log' = [rep_log EXCEPT ![response.dest] = @ \union { response.record }]
            /\ rep_leo' = [rep_leo EXCEPT ![response.dest] = response.record.offset]
            /\ UNCHANGED << rep_state, rep_epoch, rep_leader, rep_max_leo_of_rep, CoordVars, 
                            confirmed, start_stop_ctr, requests >>
        
QuorumWritten(record, leader) ==
    Cardinality({ r \in R : rep_max_leo_of_rep[leader][r] >= record.offset}) 
                >= (Cardinality(R) \div 2) + 1

\* A leader replica will confirm a write if a quorum of replicas have confirmed 
\* they have it (by including their LEO in their fetch requests)
ConfirmWrite ==
    \E r \in R :
        /\ rep_state[r] = "leader"
        /\ \E record \in rep_log[r] :
            /\ confirmed[record.value] = FALSE
            /\ QuorumWritten(record, r)
            /\ confirmed' = [confirmed EXCEPT ![record.value] = TRUE]
        /\ UNCHANGED << RepVars, CoordVars, start_stop_ctr, requests >>                



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
    \/ ReplicaSendsFetchRequest
    \/ LeaderReplicaHandlesFetchRequest
    \/ ReplicaHandlesFetchResponse
    \/ ConfirmWrite
    
\*-------------------------------------------------    
\*INVARIANTS
\*-------------------------------------------------

Record == [offset: Nat, epoch: Nat, value: V]
RepState == { "offline", "fenced", "pending_truncate", "follower", "leader" }
ReplicaOrNone == { 0 } \union R
RepAckedValues == {PendingFencingRequest, PendingFencingResponse } \union Nat
ElectionPhases == { "fencing", "notify_leader", "pending_leader_confirm", "notify_followers" }

TypeOK ==
    /\ rep_state \in [R -> RepState]
    /\ rep_leader \in [R -> ReplicaOrNone]
    /\ rep_log \in [R -> SUBSET Record]
    /\ rep_leo \in [R -> Nat]
    /\ rep_epoch \in [R -> Nat]
    /\ rep_max_leo_of_rep \in [R -> [R -> RepAckedValues]]
    /\ coord_state \in [R -> RepState]
    /\ coord_epoch \in Nat
    /\ coord_leader \in ReplicaOrNone
    /\ \A epoch \in DOMAIN coord_election : coord_election[epoch] \in ElectionPhases
    /\ start_stop_ctr \in Nat

\* There cannot exist a record in non-stale follower that does not exist in the leader        
NoDivergence ==
    \/ coord_leader = 0
    \/ /\ coord_leader # 0
       /\ \A r \in R :
            IF rep_state[r] = "follower" /\ rep_epoch[r] = rep_epoch[coord_leader] THEN
                /\ \A record \in rep_log[r] : record \in rep_log[coord_leader]
                /\ \A record \in rep_log[coord_leader] :
                    \/ record.offset > rep_leo[r]
                    \/ record \in rep_log[r] 
            ELSE
                TRUE

FollowerEqualOrLowerEpoch ==
     \/ coord_leader = 0
     \/ /\ coord_leader # 0
        /\ \A r \in R :
            \/ rep_state[r] # "follower"
            \/ /\ rep_state[r] = "follower"
               /\ rep_epoch[r] <= rep_epoch[coord_leader]
    
\* There cannot be a confirmed writw that does not exist in the leader's log
NoLossOfConfirmedWrite ==
    IF coord_leader # 0 /\ Cardinality(DOMAIN confirmed) > 0 THEN
        \A v \in DOMAIN confirmed :
            \/ /\ confirmed[v] = TRUE
               /\ \E record \in rep_log[coord_leader] : record.value = v
            \/ confirmed[v] = FALSE
    ELSE
        TRUE
        
LeoMatchesLog ==
    \A r \in R :
        IF rep_log[r] = {} THEN
            rep_leo[r] = 0
        ELSE
            LET last_record == CHOOSE record \in rep_log[r] :
                                \A rec \in rep_log[r] : record.offset >= rec.offset
            IN rep_leo[r] = last_record.offset


TestInv ==
    \* Add debugging expressions here to trigger error trace
    TRUE


=============================================================================
\* Modification History
\* Last modified Thu Jan 30 02:05:37 PST 2020 by jack
\* Last modified Wed Jan 29 16:04:31 CET 2020 by GUNMETAL
\* Created Mon Jan 27 18:48:47 CET 2020 by GUNMETAL
