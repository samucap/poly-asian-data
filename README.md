# Data syncing/processing pipelines

    - Data fetching and processing pipelines from various sources (only polymarket for now) and processed for different applications

#### TODO:

    - catalog-markets pipeline still suboptimal with tags parentTagID association. sports-sync needs to generate an accurate sports > sportName > leagues > teams hierarchies so that each has proper parentTagID (upper most next).
    many of the tags are just defaulting to sports tagID as parent. re-match teams after catalog has written event tags, still before Aggregate/UpdateTags, and only when both ends of the FK exist.
    No need to reopen the whole hierarchy design unless something else breaks.
