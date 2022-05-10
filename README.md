# Add Metadata to Twitter User Objects

- [add_metadata_insert_database.py](https://github.com/politusanalytics/add-metadata-insert-database/blob/main/add_metadata_insert_database.py): Read user objects from twitter_db.user_collection, add metadata ('following', 'followers', 'tweets', 'follower_count', 'following_count', 'downloaded', 'pp'), insert into twitter_db.twitter_collection

- [add_metadata_write_daily.py](https://github.com/politusanalytics/add-metadata-insert-database/blob/main/add_metadata_write_daily.py): Read user objects from 100.000 user batches, add metadata ('following', 'followers', 'tweets', 'follower_count', 'following_count', 'downloaded', 'pp'), write to daily files
