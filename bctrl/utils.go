package bctrl

import (
	"crypto/md5"
	"fmt"
	"io"
	"math"
)

func round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

// MD5 returns the md5 hash of strings.
func MD5(slice ...string) string {
	h := md5.New()
	for _, v := range slice {
		io.WriteString(h, v)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func SplitInteger(m, n int) (ints []int) {
	quotient := m / n
	remainder := m % n
	if remainder >= 0 {
		for i := 0; i < n-remainder; i++ {
			ints = append(ints, quotient)
		}
		for i := 0; i < remainder; i++ {
			ints = append(ints, quotient+1)
		}
		return
	} else if remainder < 0 {
		for i := 0; i < -remainder; i++ {
			ints = append(ints, quotient-1)
		}
		for i := 0; i < n+remainder; i++ {
			ints = append(ints, quotient)
		}
	}
	return
}
