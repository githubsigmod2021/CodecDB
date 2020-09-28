clear;
load('plain_int');

edge= [0,0.1,0.25,0.5,0.75,1,inf];
name = ["$\leq 0.1$" "$\leq 0.25$" "$\leq 0.5$" "$\leq 0.75$" "$\leq 1$" "$>1$"];
num_bins = 6;

enc_ratio = enc_gen_scan.enc_size ./enc_gen_scan.plain;
gzip_ratio = gzip_gen_scan.fs ./ gzip_gen_scan.plain;
lzo_ratio = lzo_gen_scan.fs ./ lzo_gen_scan.plain;

ye = discretize(enc_ratio,edge);
yg = discretize(gzip_ratio,edge);
yl = discretize(lzo_ratio,edge);

counter = zeros(3, num_bins);

for i = 1:6
    counter(1,i) = sum(ye==i);
    counter(2,i) = sum(yg==i);
    counter(3,i) = sum(yl==i);
end

counter = cumsum(counter,2);
counter = counter ./ counter(:,num_bins);
counter = counter';
fileID = fopen('comp_size_plain_int.data','w');
for i = 1: num_bins
    fprintf(fileID,'%d,%d,%d,%d\n',i,counter(i,:));
end
fclose(fileID);


%csvwrite('comp_size_plain_int.data',counter');