package mesh

import "golang.org/x/sys/windows"

func directWaitBenchmarkCPU() int64 {
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &created, &exited, &kernel, &user); err != nil {
		panic(err)
	}
	ticks := func(t windows.Filetime) int64 {
		return int64(uint64(t.HighDateTime)<<32|uint64(t.LowDateTime)) * 100
	}
	return ticks(kernel) + ticks(user)
}
