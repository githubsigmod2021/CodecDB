# Pattern Mining

Pattern mining targets at looking for a common pattern describing the dataset. Here without loss of generality, we assume the dataset is a collection of strings, each representing a data record.

## Pattern Generation
The process of generating patterns from a collection of given token sequence is comprised of the following steps:

1. **Tokenize** Using a lexer to convert each line to a list of token streams. The tokens can be either *PrimitiveToken*, which is a single word / number / symbol, or *GroupToken*, which is a list of tokens grouped by parenthetical symbols.
2. **Preprocessing** Apply preprocessing rules to the token sequences
3. **Generate** Create a sequence for each line of tokens. Create a Union containing each sequence. The union will be returned as generated pattern
4. **Refine** Apply refining rules repeatedly to the generated pattern until no more transformation can be applied
### Preprocessing
1. **Merging** Analyze the word frequency and combine words that **ALWAYS** appear together as a single word.
### Refine Rules

1. **Common Sequence** Find Common Sequence from the token streams. Here we define the common sequence to be a sub-list of tokens having the same type. The common sequences separate the list of token streams into sub-chunks.
1. **Common Symbols** Considering that most readable data use non-alphabetic,non-numerical characters as separator, a faster alternative to **Common Sequence** is to only look at common symbols in sequence.
2. **Frequent Similar Words** Sub-chunks do not contain common sequences and thus cannot be further separated by the steps above. To deal with this, we look for frequent similar words in those sub-chunks as separator. This further split sub-chunks into smaller sub-chunks.
3. **Merge Sequence** Remove unnecessary sequence. E.g., `Seq(Seq(a,b),Seq(x,y)) => Seq(a,b,x,y)`
4. **Remove Unnecessary Structure** 
   ~~~~
   Seq(a) => a
   Union(a) => a
   Seq(a,Empty) => Seq(a)
   Seq() => Empty
   Union() => Empty
   ~~~~
5. **Use `PAny` to Reduce Union** If an union contains too many choice, e.g., over 30% of the total records, use `PAny` to replace the `PToken`s and try to find a common subsequence in that to reduce the union size.

## Pattern Matching
Given a pattern and a sequence of tokens, Pattern matching looks for the correspondence between pattern elements and tokens. To achieve this, each pattern element is assigned a unique name. If a matching is found, a mapping between name and token is returned.

For each `PUnion`, we also record whether it is in use or not for the sake of encoding size computation.

## Pattern Evaluation
Pattern Evaluation use a pattern to encode a given dataset and compute the total size needed for the pattern and the dataset. The smaller the size is, the better this pattern is.

1. Compute the size of the pattern
2. Use the pattern to split token streams into columns
3. For each column, find a proper encoding scheme for it and encode it. A heuristic based method can also be used to speed up this process.
    1. Currently, we save a selection for each union. This implicitly encode the union column with dictionary & bitpacking.
    2. For other data columns, we use the string size as encoded data size, which imply a plain encoding.

### Compute the size of a Pattern
The size of a pattern refers to the storage size (bytes) it occupies on disk. The computation follows these rules:
1. The size of a token is its length
2. The size of a sequence is the sum of its elements plus separators plus 3 (An indicator and two separators)
3. The size of a union is the sum of its elements plus separators plus 3 (An indicator and two separators)
4. The size of special tokens is one

### Compute the size of a dataset encoded with a pattern
If a leaf token corresponds to ...
1. `PToken` or `PSymbol`, size is 0 as the information is included in the pattern
2. `PAny`, size is the token size

For each `PUnion` in use, a selector is needed depending on the content size (log_2)

If there are x `PAny`, x separators are also needed 
3. `PIntRange`, size is the delta size

### Token Size Calculation

If the token is 
1. `TSymbol` 1
2. `TInt` ceil(log_2(value)/8) compact size
3. `TDouble` 4/8 depending on the size
4. `TWord` length of the token

## Pattern Validation

Patterns generated from the steps above may be interpreted in many ways. For example, the following pattern
~~~~
Seq {
    Union {
    "ABC"
    "DDD"
    }
    "3234"
}
~~~~
can be described by either of the following regular expressions
* `(ABC|DDD)3234`
* `([A-Z]+)[0-9]+`

As both expressions accurately describe the given data samples, there's no way to prefer one description over another. Instead, we look at another set of validation samples, which are extracted independently from the original dataset.

The validator matches given pattern against the validation samples, and rewrite them when necessary. 


## Pattern Query Rewriting
