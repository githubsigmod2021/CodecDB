clear
load('int_compress');

gzip_int_good = int_compress.enc_size > int_compress.gzip_size;
lzo_int_good = int_compress.enc_size > int_compress.lzo_size;
int_features = int_compress{:,17:29};

gzip_int_classify = [int_features gzip_int_good];
lzo_int_classify = [int_features lzo_int_good];

load('str_compress');

gzip_str_good = str_compress.enc_size > str_compress.gzip_size;
lzo_str_good = str_compress.enc_size > str_compress.lzo_size;
str_features = str_compress{:,17:29};

gzip_str_classify = [str_features gzip_str_good];
lzo_str_classify = [str_features lzo_str_good];
