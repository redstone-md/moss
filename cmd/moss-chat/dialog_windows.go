//go:build windows

package main

import "github.com/sqweek/dialog"

var errDialogCancelled = dialog.ErrCancelled

func (c *chatApp) selectAttachmentSource() (string, error) {
	return dialog.File().
		Title("Select attachment").
		SetStartDir(c.downloadsDir).
		Load()
}

func (c *chatApp) selectAttachmentSavePath(fileName string) (string, error) {
	return dialog.File().
		Title("Save attachment as").
		SetStartDir(c.downloadsDir).
		SetStartFile(fileName).
		Save()
}
