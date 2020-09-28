clear;
load('subattr.mat');
% value 
value=subattrben.value5;

threshold = 2;
value2 = value(value<threshold);

[b,a] = histcounts(value2,150);
b(151) = b(150);
data = [a',b'];
csvwrite('subattr_hist_plain.data',data);

[f,x] = ecdf(value2);
dd = [x,f];
csvwrite('subattr_cdf_plain.data',dd);
