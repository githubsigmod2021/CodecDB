��
��
9
Add
x"T
y"T
z"T"
Ttype:
2	
�
	ApplyAdam
var"T�	
m"T�	
v"T�
beta1_power"T
beta2_power"T
lr"T

beta1"T

beta2"T
epsilon"T	
grad"T
out"T�"
Ttype:
2	"
use_lockingbool( "
use_nesterovbool( 
�
ArgMax

input"T
	dimension"Tidx
output"output_type"
Ttype:
2	"
Tidxtype0:
2	"
output_typetype0	:
2	
x
Assign
ref"T�

value"T

output_ref"T�"	
Ttype"
validate_shapebool("
use_lockingbool(�
R
BroadcastGradientArgs
s0"T
s1"T
r0"T
r1"T"
Ttype0:
2	
8
Cast	
x"SrcT	
y"DstT"
SrcTtype"
DstTtype
8
Const
output"dtype"
valuetensor"
dtypetype
A
Equal
x"T
y"T
z
"
Ttype:
2	
�
W

ExpandDims

input"T
dim"Tdim
output"T"	
Ttype"
Tdimtype0:
2	
4
Fill
dims

value"T
output"T"	
Ttype
>
FloorDiv
x"T
y"T
z"T"
Ttype:
2	
.
Identity

input"T
output"T"	
Ttype
o
MatMul
a"T
b"T
product"T"
transpose_abool( "
transpose_bbool( "
Ttype:

2
:
Maximum
x"T
y"T
z"T"
Ttype:	
2	�
�
Mean

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( "
Ttype:
2	"
Tidxtype0:
2	
e
MergeV2Checkpoints
checkpoint_prefixes
destination_prefix"
delete_old_dirsbool(�
<
Mul
x"T
y"T
z"T"
Ttype:
2	�

NoOp
M
Pack
values"T*N
output"T"
Nint(0"	
Ttype"
axisint 
C
Placeholder
output"dtype"
dtypetype"
shapeshape:
L
PreventGradient

input"T
output"T"	
Ttype"
messagestring 
�
Prod

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( "
Ttype:
2	"
Tidxtype0:
2	
=
RealDiv
x"T
y"T
z"T"
Ttype:
2	
[
Reshape
tensor"T
shape"Tshape
output"T"	
Ttype"
Tshapetype0:
2	
o
	RestoreV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0�
l
SaveV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0�
P
Shape

input"T
output"out_type"	
Ttype"
out_typetype0:
2	
H
ShardedFilename
basename	
shard

num_shards
filename
�
#SparseSoftmaxCrossEntropyWithLogits
features"T
labels"Tlabels	
loss"T
backprop"T"
Ttype:
2"
Tlabelstype0	:
2	
N

StringJoin
inputs*N

output"
Nint(0"
	separatorstring 
�
Sum

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( "
Ttype:
2	"
Tidxtype0:
2	
,
Tanh
x"T
y"T"
Ttype:	
2
9
TanhGrad
y"T
dy"T
z"T"
Ttype:	
2
c
Tile

input"T
	multiples"
Tmultiples
output"T"	
Ttype"

Tmultiplestype0:
2	

TruncatedNormal

shape"T
output"dtype"
seedint "
seed2int "
dtypetype:
2"
Ttype:
2	�
s

VariableV2
ref"dtype�"
shapeshape"
dtypetype"
	containerstring "
shared_namestring �
&
	ZerosLike
x"T
y"T"	
Ttype"serve*1.4.02v1.4.0-rc1-11-g130a514��
d
xPlaceholder*
dtype0*'
_output_shapes
:���������*
shape:���������
`
labelPlaceholder*
dtype0	*#
_output_shapes
:���������*
shape:���������
n
layer1/truncated_normal/shapeConst*
valueB"   �  *
dtype0*
_output_shapes
:
a
layer1/truncated_normal/meanConst*
valueB
 *    *
dtype0*
_output_shapes
: 
c
layer1/truncated_normal/stddevConst*
_output_shapes
: *
valueB
 *
�#<*
dtype0
�
'layer1/truncated_normal/TruncatedNormalTruncatedNormallayer1/truncated_normal/shape*
T0*
dtype0*
_output_shapes
:	�*
seed2 *

seed 
�
layer1/truncated_normal/mulMul'layer1/truncated_normal/TruncatedNormallayer1/truncated_normal/stddev*
T0*
_output_shapes
:	�
�
layer1/truncated_normalAddlayer1/truncated_normal/mullayer1/truncated_normal/mean*
T0*
_output_shapes
:	�

	layer1/w1
VariableV2*
shared_name *
dtype0*
_output_shapes
:	�*
	container *
shape:	�
�
layer1/w1/AssignAssign	layer1/w1layer1/truncated_normal*
use_locking(*
T0*
_class
loc:@layer1/w1*
validate_shape(*
_output_shapes
:	�
m
layer1/w1/readIdentity	layer1/w1*
_output_shapes
:	�*
T0*
_class
loc:@layer1/w1
[
layer1/zerosConst*
valueB�*    *
dtype0*
_output_shapes	
:�
w
	layer1/b1
VariableV2*
shape:�*
shared_name *
dtype0*
_output_shapes	
:�*
	container 
�
layer1/b1/AssignAssign	layer1/b1layer1/zeros*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes	
:�*
use_locking(*
T0
i
layer1/b1/readIdentity	layer1/b1*
T0*
_class
loc:@layer1/b1*
_output_shapes	
:�
�
layer1/MatMulMatMulxlayer1/w1/read*
transpose_b( *
T0*(
_output_shapes
:����������*
transpose_a( 
c

layer1/addAddlayer1/MatMullayer1/b1/read*
T0*(
_output_shapes
:����������
T
layer1/outputTanh
layer1/add*(
_output_shapes
:����������*
T0
n
layer2/truncated_normal/shapeConst*
valueB"�     *
dtype0*
_output_shapes
:
a
layer2/truncated_normal/meanConst*
valueB
 *    *
dtype0*
_output_shapes
: 
c
layer2/truncated_normal/stddevConst*
valueB
 *
�#<*
dtype0*
_output_shapes
: 
�
'layer2/truncated_normal/TruncatedNormalTruncatedNormallayer2/truncated_normal/shape*

seed *
T0*
dtype0*
_output_shapes
:	�*
seed2 
�
layer2/truncated_normal/mulMul'layer2/truncated_normal/TruncatedNormallayer2/truncated_normal/stddev*
T0*
_output_shapes
:	�
�
layer2/truncated_normalAddlayer2/truncated_normal/mullayer2/truncated_normal/mean*
_output_shapes
:	�*
T0

	layer2/w2
VariableV2*
shape:	�*
shared_name *
dtype0*
_output_shapes
:	�*
	container 
�
layer2/w2/AssignAssign	layer2/w2layer2/truncated_normal*
use_locking(*
T0*
_class
loc:@layer2/w2*
validate_shape(*
_output_shapes
:	�
m
layer2/w2/readIdentity	layer2/w2*
_output_shapes
:	�*
T0*
_class
loc:@layer2/w2
Y
layer2/zerosConst*
valueB*    *
dtype0*
_output_shapes
:
u
	layer2/b2
VariableV2*
shape:*
shared_name *
dtype0*
_output_shapes
:*
	container 
�
layer2/b2/AssignAssign	layer2/b2layer2/zeros*
use_locking(*
T0*
_class
loc:@layer2/b2*
validate_shape(*
_output_shapes
:
h
layer2/b2/readIdentity	layer2/b2*
T0*
_class
loc:@layer2/b2*
_output_shapes
:
�
layer2/MatMulMatMullayer1/outputlayer2/w2/read*
transpose_b( *
T0*'
_output_shapes
:���������*
transpose_a( 
e
layer2/outputAddlayer2/MatMullayer2/b2/read*
T0*'
_output_shapes
:���������
]
loss/cross_entropy/ShapeShapelabel*
T0	*
out_type0*
_output_shapes
:
�
 loss/cross_entropy/cross_entropy#SparseSoftmaxCrossEntropyWithLogitslayer2/outputlabel*
T0*6
_output_shapes$
":���������:���������*
Tlabels0	
T

loss/ConstConst*
valueB: *
dtype0*
_output_shapes
:
}
	loss/MeanMean loss/cross_entropy/cross_entropy
loss/Const*
_output_shapes
: *

Tidx0*
	keep_dims( *
T0
V
sgd/gradients/ShapeConst*
valueB *
dtype0*
_output_shapes
: 
X
sgd/gradients/ConstConst*
valueB
 *  �?*
dtype0*
_output_shapes
: 
e
sgd/gradients/FillFillsgd/gradients/Shapesgd/gradients/Const*
T0*
_output_shapes
: 
t
*sgd/gradients/loss/Mean_grad/Reshape/shapeConst*
valueB:*
dtype0*
_output_shapes
:
�
$sgd/gradients/loss/Mean_grad/ReshapeReshapesgd/gradients/Fill*sgd/gradients/loss/Mean_grad/Reshape/shape*
_output_shapes
:*
T0*
Tshape0
�
"sgd/gradients/loss/Mean_grad/ShapeShape loss/cross_entropy/cross_entropy*
T0*
out_type0*
_output_shapes
:
�
!sgd/gradients/loss/Mean_grad/TileTile$sgd/gradients/loss/Mean_grad/Reshape"sgd/gradients/loss/Mean_grad/Shape*#
_output_shapes
:���������*

Tmultiples0*
T0
�
$sgd/gradients/loss/Mean_grad/Shape_1Shape loss/cross_entropy/cross_entropy*
out_type0*
_output_shapes
:*
T0
g
$sgd/gradients/loss/Mean_grad/Shape_2Const*
valueB *
dtype0*
_output_shapes
: 
�
"sgd/gradients/loss/Mean_grad/ConstConst*
_output_shapes
:*
valueB: *7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1*
dtype0
�
!sgd/gradients/loss/Mean_grad/ProdProd$sgd/gradients/loss/Mean_grad/Shape_1"sgd/gradients/loss/Mean_grad/Const*7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1*
_output_shapes
: *

Tidx0*
	keep_dims( *
T0
�
$sgd/gradients/loss/Mean_grad/Const_1Const*
valueB: *7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1*
dtype0*
_output_shapes
:
�
#sgd/gradients/loss/Mean_grad/Prod_1Prod$sgd/gradients/loss/Mean_grad/Shape_2$sgd/gradients/loss/Mean_grad/Const_1*

Tidx0*
	keep_dims( *
T0*7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1*
_output_shapes
: 
�
&sgd/gradients/loss/Mean_grad/Maximum/yConst*
dtype0*
_output_shapes
: *
value	B :*7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1
�
$sgd/gradients/loss/Mean_grad/MaximumMaximum#sgd/gradients/loss/Mean_grad/Prod_1&sgd/gradients/loss/Mean_grad/Maximum/y*
T0*7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1*
_output_shapes
: 
�
%sgd/gradients/loss/Mean_grad/floordivFloorDiv!sgd/gradients/loss/Mean_grad/Prod$sgd/gradients/loss/Mean_grad/Maximum*7
_class-
+)loc:@sgd/gradients/loss/Mean_grad/Shape_1*
_output_shapes
: *
T0
�
!sgd/gradients/loss/Mean_grad/CastCast%sgd/gradients/loss/Mean_grad/floordiv*

SrcT0*
_output_shapes
: *

DstT0
�
$sgd/gradients/loss/Mean_grad/truedivRealDiv!sgd/gradients/loss/Mean_grad/Tile!sgd/gradients/loss/Mean_grad/Cast*#
_output_shapes
:���������*
T0
{
sgd/gradients/zeros_like	ZerosLike"loss/cross_entropy/cross_entropy:1*
T0*'
_output_shapes
:���������
�
Csgd/gradients/loss/cross_entropy/cross_entropy_grad/PreventGradientPreventGradient"loss/cross_entropy/cross_entropy:1*
T0*'
_output_shapes
:���������*�
message��Currently there is no way to take the second derivative of sparse_softmax_cross_entropy_with_logits due to the fused implementation's interaction with tf.gradients()
�
Bsgd/gradients/loss/cross_entropy/cross_entropy_grad/ExpandDims/dimConst*
_output_shapes
: *
valueB :
���������*
dtype0
�
>sgd/gradients/loss/cross_entropy/cross_entropy_grad/ExpandDims
ExpandDims$sgd/gradients/loss/Mean_grad/truedivBsgd/gradients/loss/cross_entropy/cross_entropy_grad/ExpandDims/dim*'
_output_shapes
:���������*

Tdim0*
T0
�
7sgd/gradients/loss/cross_entropy/cross_entropy_grad/mulMul>sgd/gradients/loss/cross_entropy/cross_entropy_grad/ExpandDimsCsgd/gradients/loss/cross_entropy/cross_entropy_grad/PreventGradient*
T0*'
_output_shapes
:���������
s
&sgd/gradients/layer2/output_grad/ShapeShapelayer2/MatMul*
T0*
out_type0*
_output_shapes
:
r
(sgd/gradients/layer2/output_grad/Shape_1Const*
dtype0*
_output_shapes
:*
valueB:
�
6sgd/gradients/layer2/output_grad/BroadcastGradientArgsBroadcastGradientArgs&sgd/gradients/layer2/output_grad/Shape(sgd/gradients/layer2/output_grad/Shape_1*2
_output_shapes 
:���������:���������*
T0
�
$sgd/gradients/layer2/output_grad/SumSum7sgd/gradients/loss/cross_entropy/cross_entropy_grad/mul6sgd/gradients/layer2/output_grad/BroadcastGradientArgs*
_output_shapes
:*

Tidx0*
	keep_dims( *
T0
�
(sgd/gradients/layer2/output_grad/ReshapeReshape$sgd/gradients/layer2/output_grad/Sum&sgd/gradients/layer2/output_grad/Shape*
T0*
Tshape0*'
_output_shapes
:���������
�
&sgd/gradients/layer2/output_grad/Sum_1Sum7sgd/gradients/loss/cross_entropy/cross_entropy_grad/mul8sgd/gradients/layer2/output_grad/BroadcastGradientArgs:1*
_output_shapes
:*

Tidx0*
	keep_dims( *
T0
�
*sgd/gradients/layer2/output_grad/Reshape_1Reshape&sgd/gradients/layer2/output_grad/Sum_1(sgd/gradients/layer2/output_grad/Shape_1*
T0*
Tshape0*
_output_shapes
:
�
1sgd/gradients/layer2/output_grad/tuple/group_depsNoOp)^sgd/gradients/layer2/output_grad/Reshape+^sgd/gradients/layer2/output_grad/Reshape_1
�
9sgd/gradients/layer2/output_grad/tuple/control_dependencyIdentity(sgd/gradients/layer2/output_grad/Reshape2^sgd/gradients/layer2/output_grad/tuple/group_deps*
T0*;
_class1
/-loc:@sgd/gradients/layer2/output_grad/Reshape*'
_output_shapes
:���������
�
;sgd/gradients/layer2/output_grad/tuple/control_dependency_1Identity*sgd/gradients/layer2/output_grad/Reshape_12^sgd/gradients/layer2/output_grad/tuple/group_deps*
T0*=
_class3
1/loc:@sgd/gradients/layer2/output_grad/Reshape_1*
_output_shapes
:
�
'sgd/gradients/layer2/MatMul_grad/MatMulMatMul9sgd/gradients/layer2/output_grad/tuple/control_dependencylayer2/w2/read*
T0*(
_output_shapes
:����������*
transpose_a( *
transpose_b(
�
)sgd/gradients/layer2/MatMul_grad/MatMul_1MatMullayer1/output9sgd/gradients/layer2/output_grad/tuple/control_dependency*
_output_shapes
:	�*
transpose_a(*
transpose_b( *
T0
�
1sgd/gradients/layer2/MatMul_grad/tuple/group_depsNoOp(^sgd/gradients/layer2/MatMul_grad/MatMul*^sgd/gradients/layer2/MatMul_grad/MatMul_1
�
9sgd/gradients/layer2/MatMul_grad/tuple/control_dependencyIdentity'sgd/gradients/layer2/MatMul_grad/MatMul2^sgd/gradients/layer2/MatMul_grad/tuple/group_deps*
T0*:
_class0
.,loc:@sgd/gradients/layer2/MatMul_grad/MatMul*(
_output_shapes
:����������
�
;sgd/gradients/layer2/MatMul_grad/tuple/control_dependency_1Identity)sgd/gradients/layer2/MatMul_grad/MatMul_12^sgd/gradients/layer2/MatMul_grad/tuple/group_deps*
T0*<
_class2
0.loc:@sgd/gradients/layer2/MatMul_grad/MatMul_1*
_output_shapes
:	�
�
)sgd/gradients/layer1/output_grad/TanhGradTanhGradlayer1/output9sgd/gradients/layer2/MatMul_grad/tuple/control_dependency*
T0*(
_output_shapes
:����������
p
#sgd/gradients/layer1/add_grad/ShapeShapelayer1/MatMul*
_output_shapes
:*
T0*
out_type0
p
%sgd/gradients/layer1/add_grad/Shape_1Const*
valueB:�*
dtype0*
_output_shapes
:
�
3sgd/gradients/layer1/add_grad/BroadcastGradientArgsBroadcastGradientArgs#sgd/gradients/layer1/add_grad/Shape%sgd/gradients/layer1/add_grad/Shape_1*
T0*2
_output_shapes 
:���������:���������
�
!sgd/gradients/layer1/add_grad/SumSum)sgd/gradients/layer1/output_grad/TanhGrad3sgd/gradients/layer1/add_grad/BroadcastGradientArgs*
_output_shapes
:*

Tidx0*
	keep_dims( *
T0
�
%sgd/gradients/layer1/add_grad/ReshapeReshape!sgd/gradients/layer1/add_grad/Sum#sgd/gradients/layer1/add_grad/Shape*
T0*
Tshape0*(
_output_shapes
:����������
�
#sgd/gradients/layer1/add_grad/Sum_1Sum)sgd/gradients/layer1/output_grad/TanhGrad5sgd/gradients/layer1/add_grad/BroadcastGradientArgs:1*
_output_shapes
:*

Tidx0*
	keep_dims( *
T0
�
'sgd/gradients/layer1/add_grad/Reshape_1Reshape#sgd/gradients/layer1/add_grad/Sum_1%sgd/gradients/layer1/add_grad/Shape_1*
T0*
Tshape0*
_output_shapes	
:�
�
.sgd/gradients/layer1/add_grad/tuple/group_depsNoOp&^sgd/gradients/layer1/add_grad/Reshape(^sgd/gradients/layer1/add_grad/Reshape_1
�
6sgd/gradients/layer1/add_grad/tuple/control_dependencyIdentity%sgd/gradients/layer1/add_grad/Reshape/^sgd/gradients/layer1/add_grad/tuple/group_deps*(
_output_shapes
:����������*
T0*8
_class.
,*loc:@sgd/gradients/layer1/add_grad/Reshape
�
8sgd/gradients/layer1/add_grad/tuple/control_dependency_1Identity'sgd/gradients/layer1/add_grad/Reshape_1/^sgd/gradients/layer1/add_grad/tuple/group_deps*
T0*:
_class0
.,loc:@sgd/gradients/layer1/add_grad/Reshape_1*
_output_shapes	
:�
�
'sgd/gradients/layer1/MatMul_grad/MatMulMatMul6sgd/gradients/layer1/add_grad/tuple/control_dependencylayer1/w1/read*'
_output_shapes
:���������*
transpose_a( *
transpose_b(*
T0
�
)sgd/gradients/layer1/MatMul_grad/MatMul_1MatMulx6sgd/gradients/layer1/add_grad/tuple/control_dependency*
transpose_b( *
T0*
_output_shapes
:	�*
transpose_a(
�
1sgd/gradients/layer1/MatMul_grad/tuple/group_depsNoOp(^sgd/gradients/layer1/MatMul_grad/MatMul*^sgd/gradients/layer1/MatMul_grad/MatMul_1
�
9sgd/gradients/layer1/MatMul_grad/tuple/control_dependencyIdentity'sgd/gradients/layer1/MatMul_grad/MatMul2^sgd/gradients/layer1/MatMul_grad/tuple/group_deps*
T0*:
_class0
.,loc:@sgd/gradients/layer1/MatMul_grad/MatMul*'
_output_shapes
:���������
�
;sgd/gradients/layer1/MatMul_grad/tuple/control_dependency_1Identity)sgd/gradients/layer1/MatMul_grad/MatMul_12^sgd/gradients/layer1/MatMul_grad/tuple/group_deps*
_output_shapes
:	�*
T0*<
_class2
0.loc:@sgd/gradients/layer1/MatMul_grad/MatMul_1
�
sgd/beta1_power/initial_valueConst*
valueB
 *fff?*
_class
loc:@layer1/b1*
dtype0*
_output_shapes
: 
�
sgd/beta1_power
VariableV2*
dtype0*
_output_shapes
: *
shared_name *
_class
loc:@layer1/b1*
	container *
shape: 
�
sgd/beta1_power/AssignAssignsgd/beta1_powersgd/beta1_power/initial_value*
validate_shape(*
_output_shapes
: *
use_locking(*
T0*
_class
loc:@layer1/b1
p
sgd/beta1_power/readIdentitysgd/beta1_power*
T0*
_class
loc:@layer1/b1*
_output_shapes
: 
�
sgd/beta2_power/initial_valueConst*
valueB
 *w�?*
_class
loc:@layer1/b1*
dtype0*
_output_shapes
: 
�
sgd/beta2_power
VariableV2*
_output_shapes
: *
shared_name *
_class
loc:@layer1/b1*
	container *
shape: *
dtype0
�
sgd/beta2_power/AssignAssignsgd/beta2_powersgd/beta2_power/initial_value*
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes
: 
p
sgd/beta2_power/readIdentitysgd/beta2_power*
T0*
_class
loc:@layer1/b1*
_output_shapes
: 
�
 layer1/w1/Adam/Initializer/zerosConst*
_class
loc:@layer1/w1*
valueB	�*    *
dtype0*
_output_shapes
:	�
�
layer1/w1/Adam
VariableV2*
shared_name *
_class
loc:@layer1/w1*
	container *
shape:	�*
dtype0*
_output_shapes
:	�
�
layer1/w1/Adam/AssignAssignlayer1/w1/Adam layer1/w1/Adam/Initializer/zeros*
_output_shapes
:	�*
use_locking(*
T0*
_class
loc:@layer1/w1*
validate_shape(
w
layer1/w1/Adam/readIdentitylayer1/w1/Adam*
T0*
_class
loc:@layer1/w1*
_output_shapes
:	�
�
"layer1/w1/Adam_1/Initializer/zerosConst*
_class
loc:@layer1/w1*
valueB	�*    *
dtype0*
_output_shapes
:	�
�
layer1/w1/Adam_1
VariableV2*
_output_shapes
:	�*
shared_name *
_class
loc:@layer1/w1*
	container *
shape:	�*
dtype0
�
layer1/w1/Adam_1/AssignAssignlayer1/w1/Adam_1"layer1/w1/Adam_1/Initializer/zeros*
use_locking(*
T0*
_class
loc:@layer1/w1*
validate_shape(*
_output_shapes
:	�
{
layer1/w1/Adam_1/readIdentitylayer1/w1/Adam_1*
_output_shapes
:	�*
T0*
_class
loc:@layer1/w1
�
 layer1/b1/Adam/Initializer/zerosConst*
_class
loc:@layer1/b1*
valueB�*    *
dtype0*
_output_shapes	
:�
�
layer1/b1/Adam
VariableV2*
	container *
shape:�*
dtype0*
_output_shapes	
:�*
shared_name *
_class
loc:@layer1/b1
�
layer1/b1/Adam/AssignAssignlayer1/b1/Adam layer1/b1/Adam/Initializer/zeros*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes	
:�*
use_locking(*
T0
s
layer1/b1/Adam/readIdentitylayer1/b1/Adam*
_class
loc:@layer1/b1*
_output_shapes	
:�*
T0
�
"layer1/b1/Adam_1/Initializer/zerosConst*
_class
loc:@layer1/b1*
valueB�*    *
dtype0*
_output_shapes	
:�
�
layer1/b1/Adam_1
VariableV2*
shape:�*
dtype0*
_output_shapes	
:�*
shared_name *
_class
loc:@layer1/b1*
	container 
�
layer1/b1/Adam_1/AssignAssignlayer1/b1/Adam_1"layer1/b1/Adam_1/Initializer/zeros*
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes	
:�
w
layer1/b1/Adam_1/readIdentitylayer1/b1/Adam_1*
_output_shapes	
:�*
T0*
_class
loc:@layer1/b1
�
 layer2/w2/Adam/Initializer/zerosConst*
_class
loc:@layer2/w2*
valueB	�*    *
dtype0*
_output_shapes
:	�
�
layer2/w2/Adam
VariableV2*
shared_name *
_class
loc:@layer2/w2*
	container *
shape:	�*
dtype0*
_output_shapes
:	�
�
layer2/w2/Adam/AssignAssignlayer2/w2/Adam layer2/w2/Adam/Initializer/zeros*
validate_shape(*
_output_shapes
:	�*
use_locking(*
T0*
_class
loc:@layer2/w2
w
layer2/w2/Adam/readIdentitylayer2/w2/Adam*
T0*
_class
loc:@layer2/w2*
_output_shapes
:	�
�
"layer2/w2/Adam_1/Initializer/zerosConst*
_class
loc:@layer2/w2*
valueB	�*    *
dtype0*
_output_shapes
:	�
�
layer2/w2/Adam_1
VariableV2*
shape:	�*
dtype0*
_output_shapes
:	�*
shared_name *
_class
loc:@layer2/w2*
	container 
�
layer2/w2/Adam_1/AssignAssignlayer2/w2/Adam_1"layer2/w2/Adam_1/Initializer/zeros*
T0*
_class
loc:@layer2/w2*
validate_shape(*
_output_shapes
:	�*
use_locking(
{
layer2/w2/Adam_1/readIdentitylayer2/w2/Adam_1*
_output_shapes
:	�*
T0*
_class
loc:@layer2/w2
�
 layer2/b2/Adam/Initializer/zerosConst*
_class
loc:@layer2/b2*
valueB*    *
dtype0*
_output_shapes
:
�
layer2/b2/Adam
VariableV2*
shared_name *
_class
loc:@layer2/b2*
	container *
shape:*
dtype0*
_output_shapes
:
�
layer2/b2/Adam/AssignAssignlayer2/b2/Adam layer2/b2/Adam/Initializer/zeros*
use_locking(*
T0*
_class
loc:@layer2/b2*
validate_shape(*
_output_shapes
:
r
layer2/b2/Adam/readIdentitylayer2/b2/Adam*
T0*
_class
loc:@layer2/b2*
_output_shapes
:
�
"layer2/b2/Adam_1/Initializer/zerosConst*
_class
loc:@layer2/b2*
valueB*    *
dtype0*
_output_shapes
:
�
layer2/b2/Adam_1
VariableV2*
shape:*
dtype0*
_output_shapes
:*
shared_name *
_class
loc:@layer2/b2*
	container 
�
layer2/b2/Adam_1/AssignAssignlayer2/b2/Adam_1"layer2/b2/Adam_1/Initializer/zeros*
use_locking(*
T0*
_class
loc:@layer2/b2*
validate_shape(*
_output_shapes
:
v
layer2/b2/Adam_1/readIdentitylayer2/b2/Adam_1*
T0*
_class
loc:@layer2/b2*
_output_shapes
:
[
sgd/Adam/learning_rateConst*
valueB
 *o�:*
dtype0*
_output_shapes
: 
S
sgd/Adam/beta1Const*
valueB
 *fff?*
dtype0*
_output_shapes
: 
S
sgd/Adam/beta2Const*
dtype0*
_output_shapes
: *
valueB
 *w�?
U
sgd/Adam/epsilonConst*
valueB
 *w�+2*
dtype0*
_output_shapes
: 
�
#sgd/Adam/update_layer1/w1/ApplyAdam	ApplyAdam	layer1/w1layer1/w1/Adamlayer1/w1/Adam_1sgd/beta1_power/readsgd/beta2_power/readsgd/Adam/learning_ratesgd/Adam/beta1sgd/Adam/beta2sgd/Adam/epsilon;sgd/gradients/layer1/MatMul_grad/tuple/control_dependency_1*
_class
loc:@layer1/w1*
use_nesterov( *
_output_shapes
:	�*
use_locking( *
T0
�
#sgd/Adam/update_layer1/b1/ApplyAdam	ApplyAdam	layer1/b1layer1/b1/Adamlayer1/b1/Adam_1sgd/beta1_power/readsgd/beta2_power/readsgd/Adam/learning_ratesgd/Adam/beta1sgd/Adam/beta2sgd/Adam/epsilon8sgd/gradients/layer1/add_grad/tuple/control_dependency_1*
use_locking( *
T0*
_class
loc:@layer1/b1*
use_nesterov( *
_output_shapes	
:�
�
#sgd/Adam/update_layer2/w2/ApplyAdam	ApplyAdam	layer2/w2layer2/w2/Adamlayer2/w2/Adam_1sgd/beta1_power/readsgd/beta2_power/readsgd/Adam/learning_ratesgd/Adam/beta1sgd/Adam/beta2sgd/Adam/epsilon;sgd/gradients/layer2/MatMul_grad/tuple/control_dependency_1*
use_locking( *
T0*
_class
loc:@layer2/w2*
use_nesterov( *
_output_shapes
:	�
�
#sgd/Adam/update_layer2/b2/ApplyAdam	ApplyAdam	layer2/b2layer2/b2/Adamlayer2/b2/Adam_1sgd/beta1_power/readsgd/beta2_power/readsgd/Adam/learning_ratesgd/Adam/beta1sgd/Adam/beta2sgd/Adam/epsilon;sgd/gradients/layer2/output_grad/tuple/control_dependency_1*
use_locking( *
T0*
_class
loc:@layer2/b2*
use_nesterov( *
_output_shapes
:
�
sgd/Adam/mulMulsgd/beta1_power/readsgd/Adam/beta1$^sgd/Adam/update_layer1/w1/ApplyAdam$^sgd/Adam/update_layer1/b1/ApplyAdam$^sgd/Adam/update_layer2/w2/ApplyAdam$^sgd/Adam/update_layer2/b2/ApplyAdam*
T0*
_class
loc:@layer1/b1*
_output_shapes
: 
�
sgd/Adam/AssignAssignsgd/beta1_powersgd/Adam/mul*
validate_shape(*
_output_shapes
: *
use_locking( *
T0*
_class
loc:@layer1/b1
�
sgd/Adam/mul_1Mulsgd/beta2_power/readsgd/Adam/beta2$^sgd/Adam/update_layer1/w1/ApplyAdam$^sgd/Adam/update_layer1/b1/ApplyAdam$^sgd/Adam/update_layer2/w2/ApplyAdam$^sgd/Adam/update_layer2/b2/ApplyAdam*
T0*
_class
loc:@layer1/b1*
_output_shapes
: 
�
sgd/Adam/Assign_1Assignsgd/beta2_powersgd/Adam/mul_1*
_output_shapes
: *
use_locking( *
T0*
_class
loc:@layer1/b1*
validate_shape(
�
sgd/AdamNoOp$^sgd/Adam/update_layer1/w1/ApplyAdam$^sgd/Adam/update_layer1/b1/ApplyAdam$^sgd/Adam/update_layer2/w2/ApplyAdam$^sgd/Adam/update_layer2/b2/ApplyAdam^sgd/Adam/Assign^sgd/Adam/Assign_1
_
accuracy/prediction/dimensionConst*
value	B :*
dtype0*
_output_shapes
: 
�
accuracy/predictionArgMaxlayer2/outputaccuracy/prediction/dimension*

Tidx0*
T0*
output_type0	*#
_output_shapes
:���������
a
accuracy/EqualEqualaccuracy/predictionlabel*#
_output_shapes
:���������*
T0	
b
accuracy/CastCastaccuracy/Equal*#
_output_shapes
:���������*

DstT0*

SrcT0

X
accuracy/ConstConst*
_output_shapes
:*
valueB: *
dtype0
r
accuracy/MeanMeanaccuracy/Castaccuracy/Const*
T0*
_output_shapes
: *

Tidx0*
	keep_dims( 
�
initNoOp^layer1/w1/Assign^layer1/b1/Assign^layer2/w2/Assign^layer2/b2/Assign^sgd/beta1_power/Assign^sgd/beta2_power/Assign^layer1/w1/Adam/Assign^layer1/w1/Adam_1/Assign^layer1/b1/Adam/Assign^layer1/b1/Adam_1/Assign^layer2/w2/Adam/Assign^layer2/w2/Adam_1/Assign^layer2/b2/Adam/Assign^layer2/b2/Adam_1/Assign
P

save/ConstConst*
valueB Bmodel*
dtype0*
_output_shapes
: 
�
save/StringJoin/inputs_1Const*<
value3B1 B+_temp_193539b64f684154882f4da194382c39/part*
dtype0*
_output_shapes
: 
u
save/StringJoin
StringJoin
save/Constsave/StringJoin/inputs_1*
	separator *
N*
_output_shapes
: 
Q
save/num_shardsConst*
_output_shapes
: *
value	B :*
dtype0
\
save/ShardedFilename/shardConst*
value	B : *
dtype0*
_output_shapes
: 
}
save/ShardedFilenameShardedFilenamesave/StringJoinsave/ShardedFilename/shardsave/num_shards*
_output_shapes
: 
�
save/SaveV2/tensor_namesConst*�
value�B�B	layer1/b1Blayer1/b1/AdamBlayer1/b1/Adam_1B	layer1/w1Blayer1/w1/AdamBlayer1/w1/Adam_1B	layer2/b2Blayer2/b2/AdamBlayer2/b2/Adam_1B	layer2/w2Blayer2/w2/AdamBlayer2/w2/Adam_1Bsgd/beta1_powerBsgd/beta2_power*
dtype0*
_output_shapes
:

save/SaveV2/shape_and_slicesConst*/
value&B$B B B B B B B B B B B B B B *
dtype0*
_output_shapes
:
�
save/SaveV2SaveV2save/ShardedFilenamesave/SaveV2/tensor_namessave/SaveV2/shape_and_slices	layer1/b1layer1/b1/Adamlayer1/b1/Adam_1	layer1/w1layer1/w1/Adamlayer1/w1/Adam_1	layer2/b2layer2/b2/Adamlayer2/b2/Adam_1	layer2/w2layer2/w2/Adamlayer2/w2/Adam_1sgd/beta1_powersgd/beta2_power*
dtypes
2
�
save/control_dependencyIdentitysave/ShardedFilename^save/SaveV2*
T0*'
_class
loc:@save/ShardedFilename*
_output_shapes
: 
�
+save/MergeV2Checkpoints/checkpoint_prefixesPacksave/ShardedFilename^save/control_dependency*
T0*

axis *
N*
_output_shapes
:
}
save/MergeV2CheckpointsMergeV2Checkpoints+save/MergeV2Checkpoints/checkpoint_prefixes
save/Const*
delete_old_dirs(
z
save/IdentityIdentity
save/Const^save/control_dependency^save/MergeV2Checkpoints*
T0*
_output_shapes
: 
m
save/RestoreV2/tensor_namesConst*
valueBB	layer1/b1*
dtype0*
_output_shapes
:
h
save/RestoreV2/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2	RestoreV2
save/Constsave/RestoreV2/tensor_namessave/RestoreV2/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/AssignAssign	layer1/b1save/RestoreV2*
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes	
:�
t
save/RestoreV2_1/tensor_namesConst*
_output_shapes
:*#
valueBBlayer1/b1/Adam*
dtype0
j
!save/RestoreV2_1/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_1	RestoreV2
save/Constsave/RestoreV2_1/tensor_names!save/RestoreV2_1/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_1Assignlayer1/b1/Adamsave/RestoreV2_1*
_output_shapes	
:�*
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(
v
save/RestoreV2_2/tensor_namesConst*%
valueBBlayer1/b1/Adam_1*
dtype0*
_output_shapes
:
j
!save/RestoreV2_2/shape_and_slicesConst*
dtype0*
_output_shapes
:*
valueB
B 
�
save/RestoreV2_2	RestoreV2
save/Constsave/RestoreV2_2/tensor_names!save/RestoreV2_2/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_2Assignlayer1/b1/Adam_1save/RestoreV2_2*
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes	
:�
o
save/RestoreV2_3/tensor_namesConst*
valueBB	layer1/w1*
dtype0*
_output_shapes
:
j
!save/RestoreV2_3/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_3	RestoreV2
save/Constsave/RestoreV2_3/tensor_names!save/RestoreV2_3/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_3Assign	layer1/w1save/RestoreV2_3*
use_locking(*
T0*
_class
loc:@layer1/w1*
validate_shape(*
_output_shapes
:	�
t
save/RestoreV2_4/tensor_namesConst*#
valueBBlayer1/w1/Adam*
dtype0*
_output_shapes
:
j
!save/RestoreV2_4/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_4	RestoreV2
save/Constsave/RestoreV2_4/tensor_names!save/RestoreV2_4/shape_and_slices*
dtypes
2*
_output_shapes
:
�
save/Assign_4Assignlayer1/w1/Adamsave/RestoreV2_4*
use_locking(*
T0*
_class
loc:@layer1/w1*
validate_shape(*
_output_shapes
:	�
v
save/RestoreV2_5/tensor_namesConst*%
valueBBlayer1/w1/Adam_1*
dtype0*
_output_shapes
:
j
!save/RestoreV2_5/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_5	RestoreV2
save/Constsave/RestoreV2_5/tensor_names!save/RestoreV2_5/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_5Assignlayer1/w1/Adam_1save/RestoreV2_5*
use_locking(*
T0*
_class
loc:@layer1/w1*
validate_shape(*
_output_shapes
:	�
o
save/RestoreV2_6/tensor_namesConst*
dtype0*
_output_shapes
:*
valueBB	layer2/b2
j
!save/RestoreV2_6/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_6	RestoreV2
save/Constsave/RestoreV2_6/tensor_names!save/RestoreV2_6/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_6Assign	layer2/b2save/RestoreV2_6*
_output_shapes
:*
use_locking(*
T0*
_class
loc:@layer2/b2*
validate_shape(
t
save/RestoreV2_7/tensor_namesConst*#
valueBBlayer2/b2/Adam*
dtype0*
_output_shapes
:
j
!save/RestoreV2_7/shape_and_slicesConst*
_output_shapes
:*
valueB
B *
dtype0
�
save/RestoreV2_7	RestoreV2
save/Constsave/RestoreV2_7/tensor_names!save/RestoreV2_7/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_7Assignlayer2/b2/Adamsave/RestoreV2_7*
_class
loc:@layer2/b2*
validate_shape(*
_output_shapes
:*
use_locking(*
T0
v
save/RestoreV2_8/tensor_namesConst*%
valueBBlayer2/b2/Adam_1*
dtype0*
_output_shapes
:
j
!save/RestoreV2_8/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_8	RestoreV2
save/Constsave/RestoreV2_8/tensor_names!save/RestoreV2_8/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_8Assignlayer2/b2/Adam_1save/RestoreV2_8*
_output_shapes
:*
use_locking(*
T0*
_class
loc:@layer2/b2*
validate_shape(
o
save/RestoreV2_9/tensor_namesConst*
valueBB	layer2/w2*
dtype0*
_output_shapes
:
j
!save/RestoreV2_9/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_9	RestoreV2
save/Constsave/RestoreV2_9/tensor_names!save/RestoreV2_9/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_9Assign	layer2/w2save/RestoreV2_9*
use_locking(*
T0*
_class
loc:@layer2/w2*
validate_shape(*
_output_shapes
:	�
u
save/RestoreV2_10/tensor_namesConst*#
valueBBlayer2/w2/Adam*
dtype0*
_output_shapes
:
k
"save/RestoreV2_10/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_10	RestoreV2
save/Constsave/RestoreV2_10/tensor_names"save/RestoreV2_10/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_10Assignlayer2/w2/Adamsave/RestoreV2_10*
_class
loc:@layer2/w2*
validate_shape(*
_output_shapes
:	�*
use_locking(*
T0
w
save/RestoreV2_11/tensor_namesConst*%
valueBBlayer2/w2/Adam_1*
dtype0*
_output_shapes
:
k
"save/RestoreV2_11/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_11	RestoreV2
save/Constsave/RestoreV2_11/tensor_names"save/RestoreV2_11/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_11Assignlayer2/w2/Adam_1save/RestoreV2_11*
_output_shapes
:	�*
use_locking(*
T0*
_class
loc:@layer2/w2*
validate_shape(
v
save/RestoreV2_12/tensor_namesConst*$
valueBBsgd/beta1_power*
dtype0*
_output_shapes
:
k
"save/RestoreV2_12/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_12	RestoreV2
save/Constsave/RestoreV2_12/tensor_names"save/RestoreV2_12/shape_and_slices*
dtypes
2*
_output_shapes
:
�
save/Assign_12Assignsgd/beta1_powersave/RestoreV2_12*
_output_shapes
: *
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(
v
save/RestoreV2_13/tensor_namesConst*
_output_shapes
:*$
valueBBsgd/beta2_power*
dtype0
k
"save/RestoreV2_13/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
�
save/RestoreV2_13	RestoreV2
save/Constsave/RestoreV2_13/tensor_names"save/RestoreV2_13/shape_and_slices*
_output_shapes
:*
dtypes
2
�
save/Assign_13Assignsgd/beta2_powersave/RestoreV2_13*
use_locking(*
T0*
_class
loc:@layer1/b1*
validate_shape(*
_output_shapes
: 
�
save/restore_shardNoOp^save/Assign^save/Assign_1^save/Assign_2^save/Assign_3^save/Assign_4^save/Assign_5^save/Assign_6^save/Assign_7^save/Assign_8^save/Assign_9^save/Assign_10^save/Assign_11^save/Assign_12^save/Assign_13
-
save/restore_allNoOp^save/restore_shard"<
save/Const:0save/Identity:0save/restore_all (5 @F8"�
trainable_variables��
L
layer1/w1:0layer1/w1/Assignlayer1/w1/read:02layer1/truncated_normal:0
A
layer1/b1:0layer1/b1/Assignlayer1/b1/read:02layer1/zeros:0
L
layer2/w2:0layer2/w2/Assignlayer2/w2/read:02layer2/truncated_normal:0
A
layer2/b2:0layer2/b2/Assignlayer2/b2/read:02layer2/zeros:0"
train_op


sgd/Adam"�

	variables�
�

L
layer1/w1:0layer1/w1/Assignlayer1/w1/read:02layer1/truncated_normal:0
A
layer1/b1:0layer1/b1/Assignlayer1/b1/read:02layer1/zeros:0
L
layer2/w2:0layer2/w2/Assignlayer2/w2/read:02layer2/truncated_normal:0
A
layer2/b2:0layer2/b2/Assignlayer2/b2/read:02layer2/zeros:0
d
sgd/beta1_power:0sgd/beta1_power/Assignsgd/beta1_power/read:02sgd/beta1_power/initial_value:0
d
sgd/beta2_power:0sgd/beta2_power/Assignsgd/beta2_power/read:02sgd/beta2_power/initial_value:0
d
layer1/w1/Adam:0layer1/w1/Adam/Assignlayer1/w1/Adam/read:02"layer1/w1/Adam/Initializer/zeros:0
l
layer1/w1/Adam_1:0layer1/w1/Adam_1/Assignlayer1/w1/Adam_1/read:02$layer1/w1/Adam_1/Initializer/zeros:0
d
layer1/b1/Adam:0layer1/b1/Adam/Assignlayer1/b1/Adam/read:02"layer1/b1/Adam/Initializer/zeros:0
l
layer1/b1/Adam_1:0layer1/b1/Adam_1/Assignlayer1/b1/Adam_1/read:02$layer1/b1/Adam_1/Initializer/zeros:0
d
layer2/w2/Adam:0layer2/w2/Adam/Assignlayer2/w2/Adam/read:02"layer2/w2/Adam/Initializer/zeros:0
l
layer2/w2/Adam_1:0layer2/w2/Adam_1/Assignlayer2/w2/Adam_1/read:02$layer2/w2/Adam_1/Initializer/zeros:0
d
layer2/b2/Adam:0layer2/b2/Adam/Assignlayer2/b2/Adam/read:02"layer2/b2/Adam/Initializer/zeros:0
l
layer2/b2/Adam_1:0layer2/b2/Adam_1/Assignlayer2/b2/Adam_1/read:02$layer2/b2/Adam_1/Initializer/zeros:0*�
serving_defaultu
#
input
x:0���������2
output(
accuracy/prediction:0	���������tensorflow/serving/predict