#!/usr/bin/env python3
"""
Generate configurable number of files with configurable rows
mimicking Cassandra table schema.
"""

import argparse
import csv
import json
import os
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List
import uuid
import bisect


def generate_random_text(length: int) -> str:
    """Generate random text of specified length."""
    return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))


def generate_realistic_text(length: int) -> str:
    """Generate more realistic text with words."""
    words = []
    current_length = 0
    while current_length < length:
        word_len = random.randint(3, 12)
        word = ''.join(random.choices(string.ascii_letters, k=word_len))
        if current_length + word_len + 1 > length:
            word = word[:length - current_length - 1]
            words.append(word)
            break
        words.append(word)
        current_length += word_len + 1
    return ' '.join(words)


def generate_embedding_vector(dimensions: int = 1536) -> str:
    """Generate a random embedding vector as comma-separated floats."""
    # Generate random floats between -1 and 1 (typical embedding range)
    vector = [f"{random.uniform(-1.0, 1.0):.6f}" for _ in range(dimensions)]
    return ','.join(vector)


def generate_timestamp_text() -> str:
    """Generate a random timestamp as text."""
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def generate_date_text() -> str:
    """Generate a random date as text (date only, no time)."""
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime('%Y-%m-%d')


def generate_uuid(use_random: bool = True, base_uuid_int: int = None) -> str:
    """Generate a UUID."""
    if use_random:
        return str(uuid.uuid4())
    else:
        return str(uuid.UUID(int=base_uuid_int))


def generate_realistic_domain() -> str:
    """Generate a realistic domain name."""
    tlds = ['com', 'org', 'net', 'io', 'edu', 'gov', 'co', 'ai', 'tech']
    domain = ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 12)))
    return f"{domain}.{random.choice(tlds)}"


def generate_realistic_url(base_domain: str) -> str:
    """Generate a realistic URL."""
    protocols = ['https://', 'http://']
    protocol = random.choice(protocols)
    path = '/'.join(''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 8))) for _ in range(random.randint(1, 3)))
    return f"{protocol}{base_domain}/{path}"


# Production-realistic size distribution (max_size, relative_frequency)
SIZE_DISTRIBUTION = [
    (3718, 5.620), (4236, 19.480), (4754, 14.260), (5272, 9.210), (5790, 4.320),
    (6308, 1.610), (6826, 0.870), (7343, 1.170), (7861, 1.970), (8379, 1.980),
    (8897, 1.160), (9415, 1.020), (9933, 1.710), (10451, 2.120), (10969, 1.760),
    (11487, 1.390), (12005, 1.100), (12523, 1.200), (13041, 1.230), (13559, 1.370),
    (14077, 1.110), (14594, 0.920), (15112, 0.810), (15630, 0.870), (16148, 0.900),
    (16666, 0.810), (17184, 0.680), (17702, 0.650), (18220, 0.630), (18738, 0.630),
    (19256, 0.620), (19774, 0.550), (20292, 0.510), (20810, 0.540), (21328, 0.510),
    (21846, 0.480), (22363, 0.490), (22881, 0.380), (23399, 0.400), (23917, 0.350),
    (24435, 0.340), (24953, 0.310), (25471, 0.330), (25989, 0.300), (26507, 0.270),
    (27025, 0.270), (27543, 0.330), (28061, 0.270), (28579, 0.260), (29097, 0.230),
    (29614, 0.210), (30132, 0.230), (30650, 0.200), (31168, 0.180), (31686, 0.170),
    (32204, 0.180), (32722, 0.160), (33240, 0.180), (33758, 0.150), (34276, 0.160),
    (34794, 0.150), (35312, 0.150), (35830, 0.160), (36348, 0.120), (36866, 0.150),
    (37383, 0.130), (37901, 0.160), (38419, 0.140), (38937, 0.110), (39455, 0.110),
    (39973, 0.130), (40491, 0.110), (41009, 0.130), (41527, 0.120), (42045, 0.110),
    (42563, 0.130), (43081, 0.100), (43599, 0.100), (44117, 0.090), (44634, 0.110),
    (45152, 0.070), (45670, 0.090), (46188, 0.090), (46706, 0.080), (47224, 0.090),
    (47742, 0.080), (48260, 0.070), (48778, 0.090), (49296, 0.070), (49814, 0.060),
    (50332, 0.060), (50850, 0.070), (51368, 0.070), (51886, 0.060), (52403, 0.060),
    (52921, 0.040), (53439, 0.060), (53957, 0.050), (54475, 0.060), (54993, 0.040),
    (55511, 0.060), (56029, 0.050), (56547, 0.050), (57065, 0.040), (57583, 0.050),
    (58101, 0.060), (58619, 0.030), (59137, 0.040), (59654, 0.050), (60172, 0.050),
    (60690, 0.040), (61208, 0.040), (61726, 0.040), (62244, 0.030), (62762, 0.040),
    (63280, 0.040), (63798, 0.030), (64316, 0.040), (64834, 0.030), (65352, 0.050),
    (65870, 0.030), (66388, 0.040), (66906, 0.040), (67423, 0.020), (67941, 0.030),
    (68459, 0.030), (68977, 0.030), (69495, 0.030), (70013, 0.040), (70531, 0.030),
    (71049, 0.030), (71567, 0.030), (72085, 0.030), (72603, 0.040), (73121, 0.040),
    (73639, 0.030), (74157, 0.030), (74674, 0.020), (75192, 0.030), (75710, 0.020),
    (76228, 0.030), (76746, 0.020), (77264, 0.020), (77782, 0.030), (78300, 0.020),
    (78818, 0.020), (79336, 0.020), (79854, 0.020), (80372, 0.020), (80890, 0.020),
    (81408, 0.020), (81926, 0.030), (82443, 0.020), (82961, 0.020), (83479, 0.020),
    (83997, 0.020), (84515, 0.020), (85033, 0.020), (85551, 0.020), (86069, 0.020),
    (86587, 0.010), (87105, 0.010), (87623, 0.030), (88141, 0.030), (88659, 0.010),
    (89177, 0.020), (89694, 0.020), (90212, 0.020), (90730, 0.020), (91248, 0.020),
    (91766, 0.020), (92284, 0.020), (92802, 0.020), (93320, 0.010), (93838, 0.010),
    (94356, 0.010), (94874, 0.010), (95392, 0.030), (95910, 0.010), (96428, 0.020),
    (96946, 0.010), (97463, 0.020), (97981, 0.010), (98499, 0.020), (99017, 0.010),
    (99535, 0.020), (100053, 0.010), (100571, 0.010), (101089, 0.020), (101607, 0.010),
    (102125, 0.010), (102643, 0.010), (103161, 0.010), (103679, 0.010), (104197, 0.020),
    (104714, 0.020), (105232, 0.020), (105750, 0.020), (106268, 0.020), (106786, 0.020),
    (107304, 0.020), (107822, 0.020), (108340, 0.010), (108858, 0.020), (109376, 0.010),
    (109894, 0.020), (110412, 0.010), (110930, 0.030), (111448, 0.020), (111966, 0.030),
    (112483, 0.030), (113001, 0.030), (113519, 0.020), (114037, 0.030), (114555, 0.020),
    (115073, 0.020), (115591, 0.020), (116109, 0.020), (116627, 0.030), (117145, 0.030),
    (117663, 0.020), (118181, 0.010), (118699, 0.020), (119217, 0.020), (119734, 0.010),
    (120252, 0.030), (120770, 0.020), (121288, 0.020), (121806, 0.020), (122324, 0.020),
    (122842, 0.020), (123360, 0.010), (123878, 0.030), (124396, 0.020), (124914, 0.020),
    (125432, 0.030), (125950, 0.040), (126468, 0.020), (126986, 0.030), (127503, 0.020),
    (128021, 0.020), (128539, 0.010), (129057, 0.020), (129575, 0.030), (130093, 0.010),
    (130611, 0.020), (131129, 0.020), (131647, 0.010), (132165, 0.010), (132683, 0.020),
    (133201, 0.020), (133719, 0.020), (134237, 0.020), (134754, 0.010), (135272, 0.010),
    (135790, 0.010), (136308, 0.010), (136826, 0.010), (137344, 0.020), (137862, 0.010),
    (138380, 0.010), (138898, 0.020), (139416, 0.010), (139934, 0.010), (140452, 0.010),
    (140970, 0.010), (141488, 0.010), (142006, 0.010), (142523, 0.010), (143559, 0.010),
    (144595, 0.010), (145113, 0.010), (147703, 0.010), (149774, 0.010), (151328, 0.010),
]


def sample_size_from_distribution() -> int:
    """Sample a row size from the production distribution using weighted random selection."""
    sizes = [s for s, _ in SIZE_DISTRIBUTION]
    weights = [w for _, w in SIZE_DISTRIBUTION]
    return random.choices(sizes, weights=weights, k=1)[0]


def generate_row(
    key_index: int, 
    use_random_uuid: bool = True,
    target_min_bytes: int = 200000, 
    target_max_bytes: int = 400000
) -> List[str]:
    """Generate a single row that meets the target size requirements."""
    
    # 1. sort_key - UUID
    sort_key = generate_uuid(use_random=use_random_uuid, base_uuid_int=key_index)
    
    # 2. Timestamps (fixed size)
    metadata_content_updated_ts = generate_timestamp_text()
    metadata_crawled_ts = generate_timestamp_text()
    metadata_published_date = generate_date_text()
    metadata_updated_ts = generate_timestamp_text()
    
    # 3. Bigint timestamps
    now_ms = int(time.time() * 1000)
    expire_at = now_ms + random.randint(86400000, 31536000000)
    updated_at = now_ms
    
    # 4. Generate realistic base values for text fields
    domain = generate_realistic_domain()
    url_origin = generate_realistic_url(domain)
    
    # Generate a different random URL for the normalized URL field
    domain2 = generate_realistic_domain()
    scrape_info_teapot_normalized_url = generate_realistic_url(domain2)
    
    content_title = generate_realistic_text(random.randint(20, 60))
    metadata_author = generate_realistic_text(random.randint(10, 30))
    metadata_hostname = domain
    metadata_sitename = ''.join(random.choices(string.ascii_letters, k=random.randint(8, 20)))
    scrape_info_consumer_name = random.choice(['scraper_v1', 'crawler_agent', 'harvester_bot', 'spider_v2'])
    system_info_content_type = random.choice(['text/html', 'application/json', 'text/plain', 'application/xml'])
    
    # Create row with base values
    row = [
        sort_key,
        '',  # content_embedding_chunks - will fill
        '',  # content_text_chunks - will fill  
        content_title,
        domain,
        url_origin,
        str(expire_at),
        metadata_author,
        metadata_content_updated_ts,
        metadata_crawled_ts,
        '',  # metadata_description - will fill
        metadata_hostname,
        metadata_published_date,
        metadata_sitename,
        metadata_updated_ts,
        scrape_info_consumer_name,
        scrape_info_teapot_normalized_url,
        system_info_content_type,
        str(updated_at),
    ]
    
    # Calculate current size
    current_size = sum(len(f) for f in row)
    
    # Target size - use distribution if no explicit range provided
    if target_min_bytes == 200000 and target_max_bytes == 400000:
        # Default values - use production distribution
        target_size = sample_size_from_distribution()
    else:
        # Explicit range provided - use it
        target_size = random.randint(target_min_bytes, target_max_bytes)
    
    available_space = max(0, target_size - current_size)
    
    # Allocate available space:
    # - 40% to description (single large text field)
    # - 30% to content_text_chunks (multiple text chunks, comma-separated in CSV)
    # - 20% to content_embedding_chunks (multiple blob chunks, comma-separated hex in CSV)
    # - 10% to other expandable fields
    
    desc_space = int(available_space * 0.40)
    text_chunks_space = int(available_space * 0.30)
    embed_chunks_space = int(available_space * 0.20)
    other_space = available_space - desc_space - text_chunks_space - embed_chunks_space
    
    # Fill metadata_description
    row[10] = generate_realistic_text(desc_space)
    
    # Fill content_text_chunks (comma-separated text chunks)
    num_text_chunks = random.randint(3, 6)
    chunk_size = text_chunks_space // num_text_chunks
    text_chunks = [generate_realistic_text(chunk_size) for _ in range(num_text_chunks)]
    row[2] = ','.join(text_chunks)  # Comma-separated within the CSV field
    
    # Fill content_embedding_chunks (multiple embedding vectors)
    # Each vector will be stored as comma-separated floats
    # We need to estimate how many embeddings fit in the space
    # Each float value is ~8 chars (e.g., "0.123456"), plus comma separators
    embed_dimensions = random.choice([512, 768, 1024, 1536])  # Common embedding sizes
    floats_per_vector = embed_dimensions
    chars_per_vector = floats_per_vector * 9  # ~9 chars per float value (including comma)
    
    num_embed_chunks = max(1, embed_chunks_space // chars_per_vector)
    num_embed_chunks = min(num_embed_chunks, 5)  # Cap at 5 chunks
    
    embed_vectors = [generate_embedding_vector(embed_dimensions) for _ in range(num_embed_chunks)]
    # Store as pipe-separated since vectors themselves are comma-separated
    row[1] = '|'.join(embed_vectors)  # Pipe-separated vectors within the CSV field
    
    # Distribute remaining space to other text fields
    remaining = other_space
    expandable_fields = [
        (3, 'title'),
        (7, 'author'),
        (11, 'hostname'),
        (13, 'sitename'),
    ]
    
    for field_idx, field_name in expandable_fields:
        if remaining <= 100:
            break
        max_field_space = min(remaining // 2, 5000)
        if max_field_space < 100:
            break
        field_space = random.randint(100, max_field_space)
        expanded = generate_realistic_text(field_space)
        row[field_idx] = f"{row[field_idx]} {expanded}"
        remaining -= field_space
    
    return row


def generate_file(file_num: int, worker_id: int, output_dir: str, rows_per_file: int, start_key: int, 
                  use_random_uuid: bool, min_size: int, max_size: int, headers: List[str]) -> Dict:
    """Generate a single CSV file (for parallel execution)."""
    filename = os.path.join(output_dir, f"worker{worker_id:02d}_data_{file_num:04d}.csv")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        
        sizes = []
        current_key = start_key
        
        for row_num in range(rows_per_file):
            row = generate_row(current_key, use_random_uuid=use_random_uuid,
                              target_min_bytes=min_size, target_max_bytes=max_size)
            writer.writerow(row)
            
            # Calculate actual row size
            row_size = sum(len(field) for field in row)
            sizes.append(row_size)
            current_key += 1
    
    return {
        'file_num': file_num,
        'filename': filename,
        'rows': len(sizes),
        'min_size': min(sizes),
        'max_size': max(sizes),
        'avg_size': sum(sizes) / len(sizes),
        'start_key': start_key,
        'end_key': current_key - 1
    }


def main():
    parser = argparse.ArgumentParser(
        description='Generate configurable number of CSV files with Cassandra-like data'
    )
    parser.add_argument(
        '--num-files',
        type=int,
        default=5,
        help='Number of files to generate (default: 5)'
    )
    parser.add_argument(
        '--rows-per-file',
        type=int,
        default=10,
        help='Number of rows per file (default: 10)'
    )
    parser.add_argument(
        '--start-key',
        type=int,
        default=1,
        help='Starting key index for UUID generation (default: 1)'
    )
    parser.add_argument(
        '--incremental-uuid',
        action='store_true',
        help='Use incremental UUIDs instead of random UUIDs'
    )
    parser.add_argument(
        '--min-size',
        type=int,
        default=200000,
        help='Minimum row size in bytes (default: 200000 = 200KB)'
    )
    parser.add_argument(
        '--max-size',
        type=int,
        default=400000,
        help='Maximum row size in bytes (default: 400000 = 400KB)'
    )
    parser.add_argument(
        '--output-dir',
        default='.',
        help='Output directory for CSV files (default: current directory)'
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=4,
        help='Number of parallel threads (default: 4)'
    )
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # CSV column headers matching the Cassandra schema
    headers = [
        'sort_key',
        'content_embedding_chunks',
        'content_text_chunks',
        'content_title',
        'content_url_domain',
        'content_url_origin',
        'expire_at',
        'metadata_author',
        'metadata_content_updated_ts',
        'metadata_crawled_ts',
        'metadata_description',
        'metadata_hostname',
        'metadata_published_date',
        'metadata_sitename',
        'metadata_updated_ts',
        'scrape_info_consumer_name',
        'scrape_info_teapot_normalized_url',
        'system_info_content_type',
        'updated_at',
    ]
    
    print(f"Generating {args.num_files} files with {args.rows_per_file} rows each")
    print(f"Output directory: {args.output_dir}")
    print(f"Using {args.threads} threads")
    print()
    
    start_time = time.time()
    
    # Calculate start keys for each file and assign worker IDs
    file_configs = []
    current_key = args.start_key
    for file_num in range(1, args.num_files + 1):
        worker_id = ((file_num - 1) % args.threads) + 1  # Round-robin worker assignment
        file_configs.append({
            'file_num': file_num,
            'worker_id': worker_id,
            'output_dir': args.output_dir,
            'rows_per_file': args.rows_per_file,
            'start_key': current_key,
            'use_random_uuid': not args.incremental_uuid,
            'min_size': args.min_size,
            'max_size': args.max_size,
            'headers': headers
        })
        current_key += args.rows_per_file
    
    # Generate files in parallel
    results = []
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {executor.submit(generate_file, **config): config['file_num'] 
                   for config in file_configs}
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"✓ File {result['file_num']:>3}/{args.num_files}: {result['filename']} "
                  f"({result['rows']} rows, "
                  f"keys {result['start_key']}-{result['end_key']}, "
                  f"size {result['min_size']//1024:.1f}-{result['max_size']//1024:.1f}KB avg={result['avg_size']//1024:.1f}KB)")
    
    # Sort results by file number for summary
    results.sort(key=lambda x: x['file_num'])
    
    elapsed = time.time() - start_time
    total_rows = sum(r['rows'] for r in results)
    all_sizes = [s for r in results for s in [r['min_size'], r['max_size']]]
    
    print(f"\n{'='*80}")
    print(f"Completed in {elapsed:.1f}s")
    print(f"Total files: {args.num_files}")
    print(f"Total rows: {total_rows:,}")
    print(f"Key range: {args.start_key} to {results[-1]['end_key']}")
    print(f"Row sizes: {min(all_sizes)//1024:.1f}KB - {max(all_sizes)//1024:.1f}KB")
    print(f"Throughput: {total_rows/elapsed:.0f} rows/sec")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()


