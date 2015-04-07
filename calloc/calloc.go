package calloc

// this is a super unsafe package use at your own risk!!

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

func checkPtr(ptr unsafe.Pointer) unsafe.Pointer {
	if ptr == nil {
		panic(fmt.Errorf("Unexpected nil ptr"))
	}
	return ptr
}

// Do not put pointers to GCed data in structs allocated with this
// method. They will be collected.
func Make(t reflect.Type, length, capacity int) interface{} {
	var ptr unsafe.Pointer
	if t.Kind() != reflect.Slice {
		panic("must be a slice type")
	}
	size := t.Elem().Size()
	ptr = checkPtr(C.calloc(C.size_t(capacity), C.size_t(size)))
	s := &slice.Slice{
		Array: ptr,
		Len: length,
		Cap: capacity,
	}
	sPtr := unsafe.Pointer(s)
	v := reflect.Indirect(reflect.NewAt(t, sPtr))
	return v.Interface()
}

// Works like new. Pass it a pointer type.
func New(t reflect.Type) interface{} {
	if t.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Must be a pointer, %v", t))
	}
	size := t.Elem().Size()
	ptr := checkPtr(C.malloc(C.size_t(size)))
	return reflect.NewAt(t.Elem(), ptr).Interface()
}

// Free something allocated with this system. Never free something if
// wasn't allocated here
func Free(i interface{}) {
	v := reflect.ValueOf(i)
	t := v.Type()
	if t.Kind() == reflect.Slice {
		freeSlice(i)
		return
	}
	if t.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Must free either a pointer or a slice, %v", t))
	}
	C.free(unsafe.Pointer(v.Pointer()))
}

func freeSlice(i interface{}) {
	ptr := unsafe.Pointer(&i) // pointer to interface
	inter := (*[2]uintptr)(ptr) // unpace the interface
	s := *(*slice.Slice)(unsafe.Pointer((*inter)[1])) // grab the pointer to the slice data
	C.free(s.Array)
}

