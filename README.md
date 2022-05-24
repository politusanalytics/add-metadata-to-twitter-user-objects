# Add Metadata to Twitter User Objects

- [add_metadata_insert_database.py](https://github.com/politusanalytics/add-metadata-insert-database/blob/main/add_metadata_insert_database.py): Read user objects from twitter_db.user_collection, add metadata ('following', 'followers', 'tweets', 'follower_count', 'following_count', 'downloaded', 'pp'), insert into twitter_db.twitter_collection

- [add_metadata_write_daily.py](https://github.com/politusanalytics/add-metadata-insert-database/blob/main/add_metadata_write_daily.py): Read user objects from 100.000 user batches, add metadata ('following', 'followers', 'tweets', 'follower_count', 'following_count', 'downloaded', 'pp'), write to daily files

- [province_gender_available-metadata_statistics.ipynb](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/province_gender_available-metadata_statistics.ipynb): Exploratory analysis and descriptive statistics for the collected data (22/05/2022)

- [province_gender_available-monthly_topics.ipynb](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/province_gender_available-monthly_topics.ipynb): Monthly most common 200 words and hashtags in favorited tweets from 2021 and 2022
