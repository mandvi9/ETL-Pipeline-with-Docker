import psycopg2
import argparse
import pandas as pd

file_path = '/app/Outscraper-Data.csv'

def extract_data(file):
    data = pd.read_csv(file)
    return data

# Extract data
data = extract_data(file_path)

# Print the shape of the data
print(data)
print(data.shape)

# decided to keep the country_code and google_id columns, first one for removing all international
# second one i kept for deduplicating, seems more reliable than address
to_drop = [
    'name_for_emails', 'state', 'h3', 'plus_code', 'area_service', 'street_view', 'located_in',
    'working_hours', 'working_hours_old_format', 'other_hours', 'popular_times', 'business_status', 'about',
    'range', 'posts', 'description', 'typical_time_spent', 'verified', 'owner_id', 'owner_title', 'owner_link',
    'reservation_links', 'booking_appointment_link', 'menu_link', 'order_links', 'location_link',
    'location_reviews_link', 'place_id', 'google_id', 'cid', 'reviews_id', 'located_google_id', 'facebook',
    'instagram', 'linkedin', 'medium', 'reddit', 'skype', 'snapchat', 'telegram', 'whatsapp', 'twitter',
    'vimeo', 'youtube', 'github', 'crunchbase', 'website_title', 'website_generator', 'website_description',
    'website_keywords', 'website_has_fb_pixel', 'website_has_google_tag']

data.drop(columns=to_drop, inplace=True, axis=1)

data_correct = data[data['query'].str.contains('http|https', case=False, na=False)]
data_correct_us = data_correct[data_correct['country_code'] == 'US']
data_correct_us_ratings = data_correct_us[data_correct_us['rating'] >= 3]

if 'google_id' in data_correct_us_ratings.columns and data_correct_us_ratings['google_id'].notna().all():
    data_correct_us_rating_deduped = data_correct_us_ratings.drop_duplicates(subset=['google_id'])
else:
    data_correct_us_rating_deduped = data_correct_us_ratings.drop_duplicates(subset=['full_address'])

print(data_correct_us_rating_deduped)

# hvac_contractors = data_correct_us_rating_deduped[data_correct_us_rating_deduped['category'] == 'HVAC contractor']

# hvac_contractors_info = hvac_contractors[['name', 'email_1', 'full_address']]

# print(hvac_contractors_info)

def load_data(file_path):
    # Connect to PostgreSQL
    connection = psycopg2.connect(database="lightrfp_db",
                                  host="postgres",  # Or use 'etl_pipeline-postgres-1'
                                  user="postgres",
                                  password="123456",
                                  port="5432")
    cursor = connection.cursor()

    print("Loading data...")
    data = extract_data(file_path)

    print("Transforming data...")
    data_transform = data_correct_us_rating_deduped

    # Define table name
    table_name = "vendors_data"

    # Create table if it doesn't exist
    query_create_table = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ID SERIAL PRIMARY KEY,
            name VARCHAR(255),
            site VARCHAR(255),
            subtypes VARCHAR(255),
            category VARCHAR(255),
            type VARCHAR(255),
            phone VARCHAR(255),
            full_address VARCHAR(255),
            borough VARCHAR(255),
            street VARCHAR(255),
            city VARCHAR(255),
            postal_code VARCHAR(50),
            us_state VARCHAR(10),
            country VARCHAR(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            time_zone VARCHAR(50),
            rating DOUBLE PRECISION,
            reviews TEXT,
            reviews_link TEXT,
            reviews_tags TEXT,
            reviews_per_score_1 TEXT,
            reviews_per_score_2 TEXT,
            reviews_per_score_3 TEXT,
            reviews_per_score_4 TEXT,
            reviews_per_score_5 TEXT,
            photos_count TEXT,
            photo TEXT,
            logo TEXT,
            email_1 VARCHAR(255),
            email_1_full_name VARCHAR(255),
            email_1_first_name VARCHAR(255),
            email_1_last_name VARCHAR(255),
            email_1_title VARCHAR(255),
            email_2 VARCHAR(255),
            email_2_full_name VARCHAR(255),
            email_2_first_name VARCHAR(255),
            email_2_last_name VARCHAR(255),
            email_2_title VARCHAR(255),
            email_3 VARCHAR(255),
            email_3_full_name VARCHAR(255),
            email_3_first_name VARCHAR(255),
            email_3_last_name VARCHAR(255),
            email_3_title VARCHAR(255),
            phone_1 VARCHAR(255),
            phone_2 VARCHAR(255),
            phone_3 VARCHAR(255)
        );
        """
    cursor.execute(query_create_table)

    # Insert data into the table
    print('Loading data...')
    for index, row in data_transform.iterrows():
        # Ensure that all values are strings
        row = row.fillna('')  # Replace NaN with empty string
        row = row.astype(str)  # Convert all values to strings

        # Convert latitude, longitude, and rating to float explicitly if needed
        latitude = float(row['latitude']) if row['latitude'] else None
        longitude = float(row['longitude']) if row['longitude'] else None
        rating = float(row['rating']) if row['rating'] else None

        query_insert_value = f"""
        INSERT INTO {table_name} (name, site, subtypes, category, type, phone, full_address,
                              borough, street, city, postal_code, us_state, country,
                              latitude, longitude, time_zone, rating, reviews, reviews_link,
                              reviews_tags, reviews_per_score_1, reviews_per_score_2,
                              reviews_per_score_3, reviews_per_score_4, reviews_per_score_5,
                              photos_count, photo, logo, email_1, email_1_full_name,
                              email_1_first_name, email_1_last_name, email_1_title,
                              email_2, email_2_full_name, email_2_first_name, email_2_last_name,
                              email_2_title, email_3, email_3_full_name, email_3_first_name,
                              email_3_last_name, email_3_title, phone_1, phone_2, phone_3)
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
        """
        cursor.execute(query_insert_value, (
            row['name'], row['site'], row['subtypes'], row['category'], row['type'], row['phone'],
            row['full_address'], row['borough'], row['street'], row['city'], row['postal_code'],
            row['us_state'], row['country'], row['latitude'], row['longitude'], row['time_zone'],
            row['rating'], row['reviews'], row['reviews_link'], row['reviews_tags'],
            row['reviews_per_score_1'], row['reviews_per_score_2'], row['reviews_per_score_3'],
            row['reviews_per_score_4'], row['reviews_per_score_5'], row['photos_count'], row['photo'],
            row['logo'], row['email_1'], row['email_1_full_name'], row['email_1_first_name'],
            row['email_1_last_name'], row['email_1_title'], row['email_2'], row['email_2_full_name'],
            row['email_2_first_name'], row['email_2_last_name'], row['email_2_title'], row['email_3'],
            row['email_3_full_name'], row['email_3_first_name'], row['email_3_last_name'], row['email_3_title'],
            row['phone_1'], row['phone_2'], row['phone_3']
        ))

    connection.commit()
    cursor.close()
    connection.close()

    print("ETL success...\n")
    return "All processes completed"

if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-f", "--file", required=True, help="file path of dataset")

    # Read arguments from command line
    args = parser.parse_args()

    # Load data using the provided file path
    load_data(args.file)


# check that number of %s placeholders in query_insert_value matches
# the number of items in the tuple passed to cursor.execute().

# Ensure that all the columns you are referencing in the query_insert_value
# statement exist in the DataFrame and are correctly named.

