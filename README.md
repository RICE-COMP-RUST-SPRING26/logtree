# logtree

A persistent, append-only tree optimized for high throughput when adding nodes to a branch.

## Tree API

The `Tree` provides a **persistent, append-only, branchable log structure** backed by a Write-Ahead Log (WAL).  
All state is derived from the WAL and can be reconstructed via recovery.

---

### Core Concepts

- **Node**: A unit of data with a unique `node_id`
- **Branch**: A pointer to a chain of nodes (identified by `branch_id`)
- **Parent linkage**: Each node stores a `prev_node_id`, forming a linked structure
- **WAL-backed**: All operations are persisted before becoming visible

---

## API Overview

---

### `create_tree(path, document_uuid) -> Tree`

Creates a new tree backed by a WAL file.

- Initializes:
  - `branch_id = 0`
  - `tail_node_id = 0` (empty root branch)

```rust
let tree = Tree::create_tree(path, uuid)?;
```

### `open_tree(path, document_uuid) -> Tree`
Opens an existing tree and rebuilds state from the WAL.

```rust
let tree = Tree::open_tree(path, uuid)?;
```

### `append_to_branch(branch_id, payload) -> node_id`
Appends a new node to the tail of a branch.

```rust
let node_id = tree.append_to_branch(0, b"hello".to_vec())?;
```

### `create_branch_from_parent_node(parent_node_id) -> branch_id`
Creates a new branch starting from an existing node.

``` rust
let branch_id = tree.create_branch_from_parent_node(node_id)?;
``` 

### `get_nodes_in_range(head_node_id, tail_node_id) -> Vec<Vec<u8>>`
Returns payloads from head → tail (inclusive).

``` rust
let data = tree.get_nodes_in_range(head, tail)?;
``` 


## Running with the cli

`logtree create <file> [--uuid <hex>]` \\
Create a new tree file, optionally with a specific UUID.

`logtree append <file> --data <string> [--branch <id>]` \\
Append data to a branch (defaults to branch 0).
Returns the newly created node ID.

`logtree read <file> --head <node_id> --tail <node_id>` \\
Read all payloads from head to tail (inclusive).
The head node must be an ancestor of the tail node.

`logtree branch <file> --parent-node <node_id>` \\
Create a new branch starting from the given parent node.
Returns the new branch ID.

## Datastructure Layout

The data-structure has notions of “nodes” and “branches”.

- A branch is a single sequence of nodes, starting from the root node.  
- Each branch is either the root branch (the branch containing the root node), or has a parent node on a different branch.  
- A node *N* has a single primary branch, *b*, which is the branch that it was originally appended to. However, if another branch *b’* stems from a subsequent node in *b*, then *N* will be implicitly part of branch *b’*, since branches inherit all of the history from their parent node.

Here is a simple example:

| Branch A (root) | Branch B (Parent: A4) | Branch C (Parent: B7) |
|---|---|---|
| `Node A1 (global root)` | `Node B5` | `Node C8` |
| `Node A2` | `Node B6` | `Node C9` |
| `Node A3` | `Node B7` | `Node C10` |
| `Node A4` | `Node B8` | `Node C11` |
| `Node A5` | `Node B9` | |
| `Node A6` | | |
| `Node A7` | | |

In this case, *Branch B* is comprised of the nodes A1,A2,A3,A4,B5,B6,B7,B8,B9, and *Branch C* is comprised of the nodes A1,A2,A3,A4,B5,B6,B7,C8,C9,C10,C11.

We store the tree on-disk in a WAL consisting of node and branch creation records. We utilize in-memory branch and node indices for efficient access. On crash, we can recover the state from the WAL.

## Records

BranchCreateRecord
[1] type
[4] record_length
[1] version
[8] branch_id
[8] parent_node_id (0 if root)

NodeCreateRecord
[1] type
[4] record_length
[1] version
[8] node_id
[8] branch_id
[8] prev_node_id (0 if root node)
[4] payload_length
[-] payload


## Concurrency Patterns

We utilize locks for synchronization:
* Per-branch locking
* RwLock for reading/writing to node and branch indices
* Lock for WAL appends

We generatate node and branch IDs atomically.

We consider a node/branch as committed only after it has been appended to the WAL. 

