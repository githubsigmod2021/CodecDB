clear;
load('enc');

gzip_ratio = str_enc.gzip_size ./ str_enc.enc_size;
lzo_ratio = str_enc.lzo_size ./ str_enc.enc_size;

edge= [0,0.1,0.25,0.5,0.75,1,inf];
name = ["$\leq 0.1$" "$\leq 0.25$" "$\leq 0.5$" "$\leq 0.75$" "$\leq 1$" "$>1$"];
num_bins = 6;

yg = discretize(gzip_ratio,edge);
yl = discretize(lzo_ratio,edge);

counter = zeros(2, num_bins);

for i = 1:6
    counter(1,i) = sum(yg==i);
    counter(2,i) = sum(yl==i);
end

counter = cumsum(counter,2);
counter = counter ./ counter(:,num_bins);
counter = counter';
fileID = fopen('comp_size_enc_str.data','w');
for i = 1: num_bins
    fprintf(fileID,'%d,%d,%d\n',i,counter(i,:));
end
fclose(fileID);


%csvwrite('comp_size_plain_int.data',counter')