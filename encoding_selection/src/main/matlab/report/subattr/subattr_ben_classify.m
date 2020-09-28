clear;
load('subattr.mat');
load('classify_model.mat');
% value 

value=subattrben.value5;
% value 
subattrben.value5=[];

predict = trainedModel.predictFcn(subattrben);

value(predict==1) = [];

[b,a] = histcounts(value,150);
b(151) = b(150);
data = [a',b'];
csvwrite('subattr_hist_cl.data',data);

[f,x] = ecdf(value);
dd = [x,f];
csvwrite('subattr_cdf_cl.data',dd);
