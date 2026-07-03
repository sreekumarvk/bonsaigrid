package main

import (
	"encoding/json"
	"math/rand"

	"github.com/google/uuid"
)

type User struct {
	Uuid, Username, FirstName, LastName, Address string
}

func NewUser() *User {
	return &User{
		Uuid:      uuid.NewString(),
		Username:  randStr(10),
		FirstName: randStr(5),
		LastName:  randStr(10),
		Address:   randStr(20),
	}
}

func (u *User) JSON() []byte {
	b, _ := json.Marshal(u)
	return b
}

const letters = "abcdefghijklmnopqrstuvwxyz"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
