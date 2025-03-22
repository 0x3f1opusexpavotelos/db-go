package db

import (
	"fmt"
	"os"
)

/**
write update content as a whole
app crashes when overwriting the old file
concurrent access to file half-written
create the file if not exists
truncates the existing one  before writing the content
sync buffer to fs
*/

func SaveDataToFile(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync()
}

/**
concurrency is multi-reader-single-writer
replacing a file by renaming
fs keep a mapping from filename to file data
not touching the old file, if updated is interrupted, you can recover from the old file
concurrent read won't get half written data
renaming is atomic to concurrent readers, a readers open either the old or the new file
not to power loss, event not persistent(still in buffer)
append only log
*/

func saveDataToFile2(path string, data []btye) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	err = fp.Sync()
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

/**
a small file holding the recent updates
a large file holding the rest of the data
update go the small files first, it will be merged into large file system when it reaches a threshold
copy on write
not to destroy any old  data during an update
*/

/**

Visualizing a 2-level B+tree of a sorted sequence [1, 2, 3, 4, 6, 9, 11, 12].
	[1, 	4, 	   9]
	/		|		\
	v		v		 v
[1, 2, 3] [4, 6] [9, 11, 12]

               8 (root node)
       ┌───────┴───┐
       4       ┆   a
   ┌───┴───┐   ┆ ┌─┴───┐
   2   ┆   6   ┆ 9 ┆   c
 ┌─┴─┐ ┆ ┌─┴─┐ ┆ ┆ ┆ ┌─┴─┐
 1 ┆ 3 ┆ 5 ┆ 7 ┆ ┆ ┆ b ┆ d
 ┆ ┆ ┆ ┆ ┆ ┆ ┆ ┆ ┆ ┆ ┆ ┆ ┆
┌┴┬┴┬┴┬┴┬┴┬┴┬┴┬┴┬┴┬┴┬┴┬┴┬┴┐
│1│2│3│4│5│6│7│8│9│a│b│c│d│

Copy-on-write B-tree for safe updates

in-place updates with crash recovery(double-write)

| a=1 b=2 |
    ||  1. Save a copy of the entire updated nodes.
    \/
| a=1 b=2 |   +   | a=2 b=4 |
   data           updated copy
    ||  2. fsync the saved copies.
    \/
| a=1 b=2 |   +   | a=2 b=4 |
   data           updated copy (fsync'ed)
    ||  3. Update the data structure in-place. But we crashed here!
    \/
| ??????? |   +   | a=2 b=4 |
   data (bad)     updated copy (good)
    ||  Recovery: apply the saved copy.
    \/
| a=2 b=4 |   +   | a=2 b=4 |
   data (new)     useless now

binary trees ; each key has at least 1 incoming pointer from the parent node
whereas in a B+tree, multiple keys in a leaf node share 1 incoming pointer.


log-structure merge tree (LSM-tree)
start with 2 files: a small file holding the recent updates, and a large file holding the rest of the data
Updates go to the small file first, but it cannot grow forever; it will be merged into the large file when it reaches a threshold.
Merging 2 sorted files results in a newer, larger file that replaces the old large file and shrinks the small file.
*/
