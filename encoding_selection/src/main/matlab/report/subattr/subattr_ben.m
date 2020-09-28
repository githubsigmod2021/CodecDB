clear;
load('subattr.mat');
% value 
value=subattrben.value5;

threshold = 1000;
value2 = value(value<threshold);

[b,a] = histcounts(value,150);
b(151) = b(150);
data = [a',b'];
csvwrite('subattr_hist.data',data);

[f,x] = ecdf(value2);
dd = [x,f];
csvwrite('subattr_cdf.data',dd);