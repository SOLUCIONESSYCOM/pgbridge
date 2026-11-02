package postgres

import "testing"

// Hallazgo H41 del despliegue de la web 1.5: una publicación existente se altera, no se borra y se vuelve a crear.
func TestLaPublicacionExistenteSeAlteraSinBorrarla(t *testing.T) {
	got, err := BuildAlterPublicationSQL("pub_syledger_outbox_event", []string{"syledger.outbox_event"}, []string{"insert"})
	if err != nil {
		t.Fatalf("BuildAlterPublicationSQL: %v", err)
	}
	want := []string{
		`ALTER PUBLICATION "pub_syledger_outbox_event" SET TABLE "syledger"."outbox_event"`,
		`ALTER PUBLICATION "pub_syledger_outbox_event" SET (publish = 'insert')`,
	}
	if len(got) != len(want) {
		t.Fatalf("sentencias: %q, quiero %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sentencia %d: %q, quiero %q", i, got[i], want[i])
		}
	}
	if _, err := BuildAlterPublicationSQL("p", []string{"syledger.outbox_event"}, []string{"borrar"}); err == nil {
		t.Error("una acción inválida tiene que dar error")
	}
}

// TV2-D165: REPLICA IDENTITY FULL solo sirve para publicar UPDATE o DELETE con la fila vieja. En una tabla que solo
// publica INSERT (el outbox de syledger) hace que cada UPDATE escriba la fila vieja completa en el WAL.
func TestLaIdentidadFullSoloSiSePublicanUpdateODelete(t *testing.T) {
	cases := []struct {
		actions []string
		want    bool
	}{
		{[]string{"insert"}, false},
		{[]string{"INSERT"}, false},
		{[]string{"insert", "update"}, true},
		{[]string{"Delete"}, true},
		{[]string{"insert", "delete"}, true},
	}
	for _, tc := range cases {
		if got := NeedsIdentityFull(tc.actions); got != tc.want {
			t.Errorf("NeedsIdentityFull(%q) = %v, quiero %v", tc.actions, got, tc.want)
		}
	}
}
