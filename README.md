# Add Metadata to Twitter User Objects

- [add_metadata_insert_database.py](https://github.com/politusanalytics/add-metadata-insert-database/blob/main/add_metadata_insert_database.py): Read user objects from twitter_db.user_collection, add metadata ('following', 'followers', 'tweets', 'follower_count', 'following_count', 'downloaded', 'pp'), insert into twitter_db.twitter_collection

- [add_metadata_write_daily.py](https://github.com/politusanalytics/add-metadata-insert-database/blob/main/add_metadata_write_daily.py): Read user objects from 100.000 user batches, add metadata ('following', 'followers', 'tweets', 'follower_count', 'following_count', 'downloaded', 'pp'), write to daily files

- [province_gender_available-metadata_statistics.ipynb](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/province_gender_available-metadata_statistics.ipynb): Exploratory analysis and descriptive statistics for the collected data (03/06/2022)

- [province_gender_available-monthly_topics.ipynb](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/province_gender_available-monthly_topics.ipynb): Monthly most common 200 words and hashtags in favorited tweets from 2021 and 2022

- [province_gender_available-read_data.ipynb](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/province_gender_available-read_data.ipynb): Read data from gzip file

- [province_gender_available-detect_active_users.ipynb](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/province_gender_available-detect_active_users.ipynb): Different techniques and their results to detect active users in the collected user dataset

- [data-reports](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/tree/main/data-reports):
  - [Twitter_Data_Report-220509.pdf](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220509.pdf) | [Twitter_Data_Report-220509.pptx](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220509.pptx)
  - [Twitter_Data_Report-220515.pdf](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220515.pdf) | [Twitter_Data_Report-220515.pptx](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220515.pptx)
  - [Twitter_Data_Report-220529.pdf](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220529.pdf) | [Twitter_Data_Report-220529.pptx](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220529.pptx)
  - [Twitter_Data_Report-220603.pdf](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220603.pdf) | [Twitter_Data_Report-220603.pptx](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220603.pptx)
  - [Twitter_Data_Report-220614.pdf](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220614.pdf) | [Twitter_Data_Report-220614.pptx](https://github.com/politusanalytics/add-metadata-to-twitter-user-objects/blob/main/data-reports/Twitter_Data_Report-220614.pptx)
