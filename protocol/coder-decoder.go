package protocol

import (
	"bufio"
	"encoding/json"
	"fmt"
)

func WriteMessage(w *bufio.Writer, data any) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = w.Write(bytes)
	if err != nil {
		return nil
	}
	err = w.WriteByte('\n')
	if err != nil {
		return nil
	}
	return w.Flush()
}

func WriteProtocolHeader(w *bufio.Writer) error {
	_, err := w.WriteString(ProtocolHeader + "\n")
	if err != nil {
		return nil
	}
	return w.Flush()
}

func ReadMessage(r *bufio.Reader, pointer any) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, pointer)
}

func ReadProtocolHeader(r *bufio.Reader) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil
	}
	if line != ProtocolHeader+"\n" {
		return fmt.Errorf("Not Protocol Header, instead: %q", line)
	}
	return nil
}

func ReadEnvelope(r *bufio.Reader, env *Envelope) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, env)
}
