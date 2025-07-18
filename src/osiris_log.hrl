%% record chunk_info does not map exactly to an index record (field 'num' differs)
-record(chunk_info,
        {id :: osiris:offset(),
         timestamp :: non_neg_integer(),
         epoch :: osiris:epoch(),
         num :: non_neg_integer(),
         type :: osiris_log:chunk_type(),
         %% size of data + filter + trailer
         size :: non_neg_integer(),
         %% position in segment file
         pos :: integer()
        }).
-record(seg_info,
        {file :: file:filename_all(),
         size = 0 :: non_neg_integer(),
         index :: file:filename_all(),
         first :: undefined | #chunk_info{},
         last :: undefined | #chunk_info{}}).
