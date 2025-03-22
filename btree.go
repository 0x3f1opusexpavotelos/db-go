package db

import (
	"bytes"
	"encoding/binary"
)

/**
In a B+tree, only leaf nodes store value, and only internal nodes have children
*/

// a node is just a chunk of  bytes interpreted by this format
type BNode []byte
/**
decode the node format
| type 	| nkeys 	| 	pointers 	| 	offsets 	| key-values 	| unused |
| 2B 	| 2B 		|	nkeys*8B	|	nkeys*2B	| ... 			| 		|
| klen | vlen | key | val |
| 2B   | 2B    |... |...|
 */


const (
	BNODE_NODE = 1
	BNDOE_LEAF
)
/**
Header
 */
const HEADER = 4
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

/**
Child pointers
 */
func (node BNode) getPrt(idx uint16) uint64{
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}


func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx <= node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}


func (node BNode) offsetPos( idx uint16) uint16 {
	asset(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx - 1) // adjust for 1-based idx
}



type BTree struct {
	// root pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) []byte // read a page from disk
	new func([]byte) uint64 // allocate and write a new page
	del func(uint64)        // deallocate a page
}















func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}


func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert
		}
	case BNODE_NODE: // internal node, walk into the child node
		// ...
	}
	case BNODE_NODE:
		// recursive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(kptr), key, val)
		// after insertion, split the result
		nsplit, split := nodeSplit3(knode)
		// deallocate the old kid node
		tree.del(kptr)
		// update the kid links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	return new
}


func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}


// insert a new key or update an existing key
func (tree *BTree) Insert(key []byte, val []byte) error
// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) (bool, error)


func (tree *BTree) Insert(key []byte, val []byte) error {
	// 1. check the length limit imposed by the node format
	if err := checkLimit(key, val); err != nil {
		return err // the only way for an update to fail
	}
	// 2. create the first node
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		// later...
		tree.root = tree.new(root)
		return nil
	}
	// 3. insert the key
	node := treeInsert(tree, tree.get(tree.root), key, val)
	// 4. grow the tree if the root is split
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		// later ...
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
	return nil
}