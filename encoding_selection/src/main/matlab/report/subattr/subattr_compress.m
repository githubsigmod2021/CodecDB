clear
load('subattr_compress');

col_gzipr = sub_compress.col_gzip./sub_compress.col_enc;
col_lzor = sub_compress.col_lzo./sub_compress.col_enc;

sub_gzipr = sub_compress.sub_gzip./sub_compress.sub_enc;
sub_lzor = sub_compress.sub_lzo./sub_compress.sub_enc;

edge = [0,0.25,0.5,0.75,1,Inf];

cgrb = discretize(col_gzipr,edge);
clrb = discretize(col_lzor,edge);
sgrb = discretize(sub_gzipr,edge);
slrb = discretize(sub_lzor,edge);

count = zeros(4,5);

for i = 1:5
   count(1,i) = sum(cgrb==i);
   count(2,i) = sum(clrb==i);
   count(3,i) = sum(sgrb==i);
   count(4,i) = sum(slrb==i);
end

count = cumsum(count,2);
count = count./count(:,5);
count = count * 100;
seq = ones(1,5);
seq = cumsum(seq,2);
count = cat(1, seq,count);
count(:,5) = [];
csvwrite('subattr_compress.data',count');