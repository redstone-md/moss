//go:build unix

package mesh

import "syscall"

func directWaitBenchmarkCPU() int64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return (int64(usage.Utime.Sec)+int64(usage.Stime.Sec))*1e9 +
		(int64(usage.Utime.Usec)+int64(usage.Stime.Usec))*1000
}
