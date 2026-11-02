package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/SOLUCIONESSYCOM/pgbridge/src/config"
	"github.com/SOLUCIONESSYCOM/pgbridge/src/observability"
	"github.com/jackc/pgx/v5"
)

func BuildCreatePublicationSQL(pubName string, tables []string,
	actions []string) (string, error) {

	validActions, err := ValidatePublicationActions(actions)

	if err != nil {
		return "", err
	}

	publishVal := strings.Join(validActions, ", ")

	pubIdent := pgx.Identifier{pubName}.Sanitize()

	tableIdents := make([]string, 0, len(tables))

	for _, table := range tables {

		tableIdent, err := SanitizeTable(table)

		if err != nil {
			return "", err
		}

		tableIdents = append(tableIdents, tableIdent)
	}

	q := fmt.Sprintf(CREATE_PUBLICATION_QUERY, pubIdent,
		strings.Join(tableIdents, ", "), publishVal)

	return q, nil
}

// BuildAlterPublicationSQL deja una publicación que ya existe con las tablas y acciones del listener, sin borrarla.
// Borrarla y crearla de nuevo con el slot vivo rompe la replicación: pgoutput busca la publicación en la foto del
// catálogo de la posición del slot, donde la nueva no existe ("publication ... does not exist", 42704), y el CDC
// queda caído hasta recrear el slot (hallazgo H41 del despliegue de la web 1.5).
func BuildAlterPublicationSQL(pubName string, tables []string, actions []string) ([]string, error) {
	validActions, err := ValidatePublicationActions(actions)
	if err != nil {
		return nil, err
	}
	tableIdents := make([]string, 0, len(tables))
	for _, table := range tables {
		tableIdent, err := SanitizeTable(table)
		if err != nil {
			return nil, err
		}
		tableIdents = append(tableIdents, tableIdent)
	}
	pubIdent := pgx.Identifier{pubName}.Sanitize()
	return []string{
		fmt.Sprintf(ALTER_PUBLICATION_TABLES_QUERY, pubIdent, strings.Join(tableIdents, ", ")),
		fmt.Sprintf(ALTER_PUBLICATION_PUBLISH_QUERY, pubIdent, strings.Join(validActions, ", ")),
	}, nil
}

func PublicationExists(ctx context.Context, sqlConn *pgx.Conn, publication string) (bool, error) {
	var exists bool
	err := sqlConn.QueryRow(ctx, PUBLICATION_EXISTS_QUERY, publication).Scan(&exists)
	return exists, err
}

func DropPublication(ctx context.Context, sqlConn *pgx.Conn, publication string) error {

	q := fmt.Sprintf(DROP_PUBLICATION_QUERY, pgx.Identifier{publication}.Sanitize())

	_, err := sqlConn.Exec(ctx, q)

	return err
}

func SetTableIdentityFull(ctx context.Context, sqlConn *pgx.Conn, table string) error {

	sanitizedTable, err := SanitizeTable(table)

	if err != nil {
		return err
	}

	q := fmt.Sprintf(SET_TABLE_IDENTITY_FULL_QUERY, sanitizedTable)

	_, err = sqlConn.Exec(ctx, q)

	return err
}

// NeedsIdentityFull dice si el listener publica UPDATE o DELETE: solo entonces la tabla necesita REPLICA IDENTITY FULL
// para que el CDC lleve la fila vieja.
func NeedsIdentityFull(actions []string) bool {
	for _, action := range actions {
		switch strings.ToLower(strings.TrimSpace(action)) {
		case "update", "delete":
			return true
		}
	}
	return false
}

func CreatePublicationAndSetTableIdentityFull(ctx context.Context,
	sqlConn *pgx.Conn, listener config.Listener, logger observability.Logger) error {

	if sqlConn == nil {
		return fmt.Errorf("sql connection is nil")
	}

	// Solo con UPDATE o DELETE publicados hace falta la fila vieja completa (TV2-D165). En una tabla de solo INSERT
	// no se toca: FULL haria que cada UPDATE escriba la fila vieja en el WAL sin que nadie la lea.
	if NeedsIdentityFull(listener.Actions) {
		logger.Info(ctx, "Setting table identity full", "table", listener.Table)
		if err := SetTableIdentityFull(ctx, sqlConn, listener.Table); err != nil {
			return fmt.Errorf("set table identity full: %w", err)
		}
	} else {
		logger.Info(ctx, "Tabla de solo insert: no se cambia su REPLICA IDENTITY", "table", listener.Table)
	}

	exists, err := PublicationExists(ctx, sqlConn, listener.Publication)
	if err != nil {
		return fmt.Errorf("publication exists: %w", err)
	}
	if exists {
		// Se altera, no se borra: el slot sigue viendo la misma publicación (BuildAlterPublicationSQL).
		logger.Info(ctx, "Updating existing publication", "publication", listener.Publication)
		queries, err := BuildAlterPublicationSQL(listener.Publication, []string{listener.Table}, listener.Actions)
		if err != nil {
			return err
		}
		for _, q := range queries {
			if _, err := sqlConn.Exec(ctx, q); err != nil {
				return fmt.Errorf("alter publication: %w", err)
			}
		}
		return nil
	}

	logger.Info(ctx, "Building publication query", "publication", listener.Publication)

	publicationQuery, err := BuildCreatePublicationSQL(listener.Publication,
		[]string{listener.Table}, listener.Actions)

	if err != nil {
		return err
	}

	logger.Info(ctx, "Executing publication query", "publication", listener.Publication)
	_, err = sqlConn.Exec(ctx, publicationQuery)

	if err != nil {
		if !IsDuplicateObjectError(err) {
			return err
		}
	}

	return nil
}
