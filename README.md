# logtree

A persistent, append-only tree optimized for high throughput when adding nodes to a branch.

## Running with the cli

`logtree create <file> [--uuid <hex>]`
Create a new tree file, optionally with a specific UUID.

`logtree print <file>`
Dump the full page structure of a tree file.

`logtree append <file> --data <string> [--branch <n>]`
Append data to a branch (defaults to branch 0).

`logtree read <file> --start <seq> --end <seq> [--branch <n>]`
Read a range of entries from a branch (defaults to branch 0).

`logtree add-branch <file> --parent-branch <n> --parent-seq <seq>`
Fork a new branch from a parent branch at a given sequence number.

## Datastructure Layout

The data-structure has notions of “nodes” and “branches”.

- A branch is a single sequence of nodes, starting from the root node.  
- Each branch is either the root branch (the branch containing the root node), or has a parent node on a different branch.  
- A node *N* has a single primary branch, *b*, which is the branch that it was originally appended to. However, if another branch *b’* stems from a subsequent node in *b*, then *N* will be implicitly part of branch *b’*, since branches inherit all of the history from their parent node.

Here is a simple example:

| *`Branch A`* `(root) Node A1 (global root) Node A2 Node A3 Node A4 Node A5 Node A6 Node A7` | *`Branch B`* `Parent: A4 Node B5 Node B6 Node B7 Node B8 Node B9`  | *`Branch C`* `Parent: B7 Node C8 Node C9 Node C10 Node C11`  |
| :---- | :---- | :---- |

In this case, *Branch B* is comprised of the nodes A1,A2,A3,A4,B5,B6,B7,B8,B9, and *Branch C* is comprised of the nodes A1,A2,A3,A4,B5,B6,B7,C8,C9,C10,C11.

Branches are stored in the branch directory, a page containing a single list of pages. Branches are stored by number (the root branch is branch 0, and subsequent branches have increasing numbers). Since there are a constant number of branch entries stored per branch directory page, finding a branch is doable by dividing the branch num by the branches per page, then iterating through the branch pages that many times. However, the implementation will store an in-memory vector of branch directory pages, so it will be O(1) to find a given branch after the first time it is found.

## Page Layouts

Different types of pages:

```
FILE HEADER (page 0)
[8]  magic
[4]  version
[4]  branch_dir_pagenum (branch directory page)
[16] document_uuid

BRANCH DIRECTORY PAGE
[1]  page_type (BranchDirectory)
[1]  next_page_committed (the next_pagenum has been written)
[2]  (padding)
[4]  next_pagenum (only relevant if there are more branches)
repeated: - (24 bytes each, ~170 per page)
  [1]  branch_committed
  [1]  active_log_pagenum
  [2]  (padding)
  [4]  parent_branch_num (NA for branch number 0)
  [8]  parent_sequence_num (the sequence number of the parent node in the parent branch)
  [4]  latest_log_pagenum_0
  [4]  latest_log_pagenum_1

LOG PAGE
[1]  page_type (LogPage)
[3]  (padding)
[4]  branch_num
[4]  prev_branch_pagenum (prev op page, 0 if this is the first log page of this branch)
[4]  (padding)
[8]  first_sequence_num (seq num of the 1st node on this page, or what it will be if the page is empty)
repeated node_count times:
  [1]  committed
  [1]  storage_mode (INLINE | OVERFLOW)
  [2]  (padding)
  [4]  data_length (length of payload if INLINE, 8 if OVERFLOW)
  [data_length] payload if INLINE, overflow_page_offset if OVERFLOW
```

If a node reaches a certain size, it shouldn’t store its content inline in the log page. Instead, it should store its content in one or more overflow pages. Each overflow page holds part of the content of only one node (nodes do not share overflow pages,) but multiple overflow pages may be needed for a single node.

```
OVERFLOW PAGE
[1]  page_type (Overflow)
[8]  next_overflow_offset (next overflow page, 0 = last)
[8]  total_length
[-]  payload bytes
```

## Concurrency Patterns

When an item has a “committed” bit, that means it is considered to not exist if the committed bit is 0\. Thus, a call to perform an operation can only return once committed is set to 1\. For example, if a node is added to a branch, but the program crashes before it was committed, then the next time you append a node to that branch the partially added node will be completely overridden, as a node being uncommitted is no different from if there was no node there at all.

For double-buffered values, such as `latest_op_page_offset`, setting the value requires setting the non-active value, fsyncing it, and then swapping the bit, and then fsyncing again.
