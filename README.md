# SWPC Solar Wind Data to Delta Lake in Rust
This repo was created as a Data Engineering POC of how to use Rust to ingest streaming data. 

SWPC provides real time solar wind data with 1 min updates. This is an interesting dataset to ingest because it is not a JSON standard payload and it updates frequent enough to pose a challenge. Request payload contains data for the last hour so if there is a failure with the requests the pipeline can recover if it is within the last hour. Future update will include the posibility of using the 7 day dataset in case that a failure lasts longer. Code will also be improved to handle the large amount of files due to the 1 minute updates.

I use this data to monitor possible geomagnetic storms. Geomagnetic storms can create Aurora Borealis events so this data is useful to predict when and where to see the Northern Lights. Predicting if auroras will be visible is not an exact science but this data is a good indicator of when to go out and chase them. This data provides a window of about an hour to determine if the auroras will be visible.

Visualizations based on this data are available at jayaro.dev/aurora.