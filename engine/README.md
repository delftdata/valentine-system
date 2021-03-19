# Schema Matching Engine

This module contains the engine that performs schema matching in our system.

## Endpoints

### Atlas

*   `/matches/atlas/holistic/<table_guid>` Find matches in Atlas, of a table against all other tables.
*   `/matches/atlas/other_db/<table_guid>/<db_guid>` Find matches in Atlas, of a table against the specified database.
*   `/matches/atlas/within_db/<table_guid>` Find matches in Atlas, of a table against its own database.

### Minio

*   `/matches/minio/holistic/<table_guid>` Find matches in minio, of a table against all other tables.
*   `/matches/minio/other_db/<table_guid>/<db_guid>` Find matches in minio, of a table against the specified database.
*   `/matches/minio/within_db/<table_guid>` Find matches in minio, of a table against its own database.
*   `/matches/minio/submit_batch_job` Find matches in minio, for multiple tables and multiple algorithms.
*   `/matches/minio/ls` List Minio's buckets and files 
*   `/matches/minio/column_sample/<db_name>/<table_name>/<column_name>` Get a sample from a file that resides in Minio.
*   `/minio/create_bucket/<bucket_name>` Create a bucket.
*   `/minio/upload_file/<bucket_name>` Upload a file.

### Match results

*   `/results/finished_jobs` Get the finished jobs.
*   `/results/job_results/<job_id>` Get the results of a specific job.
*   `/results/job_runtime/<job_id>` Get the runtime of a specific job.
*   `/results/save_verified_match/<job_id>/<index>` Save a verified match.
*   `/results/discard_match/<job_id>/<index>` Delete a non-match pair.
*   `/results/delete_job/<job_id>` Delete a job.
*   `/results/verified_matches` Get the verified matches.

## Matching algorithms

Here we show the matching algorithms with their names as they should be given in the `matching_algorithm` parameter and 
underneath the algorithm names are their parameters

### Schema only

1.  `"Coma"`
    
    1.  `"strategy"` defaults at the `"COMA_OPT"` value which tells Coma that we do not have instances.
    

2.  `"Cupid"`
    
    1.  `"leaf_w_struct"` value from `[0-1]` default `0.2` and specifies how much does the structural similarity of 
        the leaves affect the similarity metric (it should be low for Relational data) .
        
    2.  `"w_struct"` same as the `leaf_w_struct` but for the non-leaf elements.
        
    3.  `"th_accept"` value from `[0-1]` default `0.7` and specifies the similarity threshold to accept matches.
        
    4.  `"th_high"` value from `[0-1]` default `0.6` and is a tuning parameter, must be larger than `"th_low"`.
        
    5.  `"th_low"` value from `[0-1]` default `0.35` and is a tuning parameter, must be smaller than `"th_high"`.
        
    6.  `"c_inc"` integer > 1 similarity tuning parameter default `1.2`.
        
    7.  `"c_dec"` integer < 1 similarity tuning parameter default `0.9`.
        
    8.  `"th_ns"` the threshold of name similarity between data-types default 0.7. Columns with matches with lower 
        data-type similarity will not be considered.
        

3.  `"SimilarityFlooding"`

### Instance only

1.  `"Coma"`
    
    1.  `"strategy"` must be set at the `"COMA_OPT_INST"` value which tells Coma that we do have instances for 
        processing.
        

2.  `"CorrelationClustering"` 
    
    1.  `"threshold1"` value from `[0.01-0.5]` default `0.15` specifying the EMD threshold of the first phase.
        
    2.  `"threshold2"` same as `"threshold1"` but for the second phase.
        
    3.  `"quantiles"`  the integer number of quantiles used by the histograms default `256` (lower for faster results, 
        higher for better accuracy).
        
    4.  `"process_num"` the integer number of processes to spawn for parallel processing default `1`.
        
    5.  `"chunk_size"` the chuck size per process default None to let the algorithm determine it.
        
    6.  `"clear_cache"` boolean specifying to clear the global rank cache or not.
    

3.  `"JaccardLevenMatcher"`
    
    1.  `"threshold_leven"` threshold specifying the distance which to match column elements with each other 
        default `0.8`.
        
    2.  `"process_num"` the integer number of processes to spawn for parallel processing default `1`.

## Build and run

1.  Run `docker build .`
2.  To run the containerized service: `docker run -p <HOST_RORT>:5000 <IMAGE_ID>`
3.  Run a REST call to the endpoint that you require.

## Module's structure

*   `algorithms` Module containing all the implemented schema matching algorithms used in the service.
    
    *   `coma` Python wrapper around 
        [COMA 3.0 Comunity edition](https://sourceforge.net/projects/coma-ce/)
       
    *   `cupid` Contains the python implementation of the paper 
        [Generic Schema Matching with Cupid
        ](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.79.4079&rep=rep1&type=pdf)
       
    *   `distribution_based` Contains the python implementation of the paper 
        [Automatic Discovery of Attributes in Relational Databases
        ](https://dl-acm-org.tudelft.idm.oclc.org/doi/pdf/10.1145/1989323.1989336)
       
    *   `jaccard_levenshtein` Contains a baseline that uses Jaccard Similarity between columns to assess their 
        correspondence score, enhanced by Levenshtein Distance.
       
    *   `similarity_flooding` Contains the python implementation of the paper 
        [Similarity Flooding: A Versatile Graph Matching Algorithmand its Application to Schema Matching
        ](http://p8090-ilpubs.stanford.edu.tudelft.idm.oclc.org/730/1/2002-1.pdf)
       

*   `data_sources` Module containing the two data sources, atlas, minio and the local filesystem

*   `utils` Module containing some utility functions used throughout the service