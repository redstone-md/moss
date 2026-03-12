//go:build !windows

package main

import "errors"

var errDialogCancelled = errors.New("dialog cancelled")

func (c *chatApp) selectAttachmentSource() (string, error) {
	return "", errors.New("system file picker is currently available only on Windows builds")
}

func (c *chatApp) selectAttachmentSavePath(fileName string) (string, error) {
	return uniqueAttachmentPath(c.downloadsDir, fileName), nil
}
