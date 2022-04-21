package gorm

import (
	"context"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func (op OpentracingPlugin) beforeCreate(db *gorm.DB) {
	op.injectBefore(db, op.opt.createOpName)
}

func (op OpentracingPlugin) beforeUpdate(db *gorm.DB) {
	op.injectBefore(db, op.opt.updateOpName)
}

func (op OpentracingPlugin) beforeQuery(db *gorm.DB) {
	op.injectBefore(db, op.opt.queryOpName)
}

func (op OpentracingPlugin) beforeDelete(db *gorm.DB) {
	op.injectBefore(db, op.opt.deleteOpName)
}

func (op OpentracingPlugin) beforeRow(db *gorm.DB) {
	op.injectBefore(db, op.opt.rowOpName)
}

func (op OpentracingPlugin) beforeRaw(db *gorm.DB) {
	op.injectBefore(db, op.opt.rawOpName)
}

func (op OpentracingPlugin) after(db *gorm.DB) {
	op.extractAfter(db)
}


type myError struct {
	errs []string
}

func (e *myError) add(stage operationStage, err error) {
	if err == nil {
		return
	}

	e.errs = append(e.errs, "stage="+stage.Name()+":"+err.Error())
}

func (e *myError) toError() error {
	if len(e.errs) == 0 {
		return nil
	}

	return e
}

func (e *myError) Error() string {
	return strings.Join(e.errs, ";")
}


const (
	_prefix      = "gorm.otel"
	_errorTagKey = "error"
)

var (
	_tableTagKey        = keyWithPrefix("table")
	_resultLogKey       = keyWithPrefix("result")
	_sqlLogKey          = keyWithPrefix("sql")
	_rowsAffectedLogKey = keyWithPrefix("rowsAffected")

	spanKey = "otel:span"
)

func keyWithPrefix(key string) string {
	return _prefix + "." + key
}

func (op OpentracingPlugin) injectBefore(db *gorm.DB, name operationName) {
	if db == nil || db.Statement == nil {
		return
	}

	ctx := db.Statement.Context
	if ctx == nil {
		return
	}

	tr := otel.Tracer("MySQL-Operation")
	ctx, span := tr.Start(ctx, string(name))

	span.SetAttributes(util.DBSystemValue)
	span.SetAttributes(util.DBNameKey.String(db.Name()))

	now := time.Now()
	db.InstanceSet("start_time", now)
	db.InstanceSet("span", span)
}

func (op OpentracingPlugin) extractAfter(db *gorm.DB) {
	if db == nil || db.Statement == nil {
		return
	}

	ctx := db.Statement.Context
	if ctx == nil {
		return
	}

	var startTime time.Time
	st, isExist := db.InstanceGet("start_time")
	if isExist {
		startTime, _ = st.(time.Time)
	}

	// 通过stmt反解SQL
	sql := db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)

	// 结束span
	span, isExist := db.InstanceGet("span")
	if spanner, ok := span.(trace.Span); isExist && ok {
		spanner.SetAttributes(util.DBStatementKey.String(sql))
		spanner.End()
	}

	log.Get(ctx).Debugf("[gorm] name:%s cost: %v sql: %s", db.Name(), time.Since(startTime), sql)
}

type errorTagHook func(span trace.Span, err error)

func defaultErrorTagHook(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}


const (
	_createOp operationName = "create"
	_updateOp operationName = "update"
	_queryOp  operationName = "query"
	_deleteOp operationName = "delete"
	_rowOp    operationName = "row"
	_rawOp    operationName = "raw"
)

type operationStage string

func (op operationStage) Name() string {
	return string(op)
}

const (
	_stageBeforeCreate operationStage = "otel:before_create"
	_stageAfterCreate  operationStage = "otel:after_create"
	_stageBeforeUpdate operationStage = "otel:before_update"
	_stageAfterUpdate  operationStage = "otel:after_update"
	_stageBeforeQuery  operationStage = "otel:before_query"
	_stageAfterQuery   operationStage = "otel:after_query"
	_stageBeforeDelete operationStage = "otel:before_delete"
	_stageAfterDelete  operationStage = "otel:after_delete"
	_stageBeforeRow    operationStage = "otel:before_row"
	_stageAfterRow     operationStage = "otel:after_row"
	_stageBeforeRaw    operationStage = "otel:before_raw"
	_stageAfterRaw     operationStage = "otel:after_raw"
)

type options struct {
	logResult        bool
	tracer           oteltrace.TracerProvider
	logSqlParameters bool
	errorTagHook     errorTagHook

	createOpName operationName
	updateOpName operationName
	queryOpName  operationName
	deleteOpName operationName
	rowOpName    operationName
	rawOpName    operationName
}

func defaultOption() *options {
	return &options{
		logResult:        false,
		tracer:           otel.GetTracerProvider(),
		logSqlParameters: true,

		createOpName: _createOp,
		updateOpName: _updateOp,
		queryOpName:  _queryOp,
		deleteOpName: _deleteOp,

		rowOpName: _rowOp,
		rawOpName: _rawOp,
	}
}

type ApplyOption func(o *options)

func WithLogResult(logResult bool) ApplyOption {
	return func(o *options) { o.logResult = logResult }
}

func WithTracer(t oteltrace.TracerProvider) ApplyOption {
	return func(o *options) {
		if t == nil {
			return
		}

		o.tracer = t
	}
}

func WithSqlParameters(logSqlParameters bool) ApplyOption {
	return func(o *options) {
		o.logSqlParameters = logSqlParameters
	}
}

type operationName string

func (op operationName) String() string {
	return string(op)
}


var (
	sfg singleflight.Group
	rwl sync.RWMutex

	dbs = map[string]*gorm.DB{}

	ErrNotFound = gorm.ErrRecordNotFound
)

func Get(ctx context.Context, name string, dsn string) (db *gorm.DB, err error) {
	rwl.RLock()
	if v, ok := dbs[name]; ok {
		db = v
		rwl.RUnlock()
		return
	}
	rwl.RUnlock()

	v, _, _ := sfg.Do(name, func() (interface{}, error) {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			return nil, err
		}

		db.Use(New(WithLogResult(false), WithSqlParameters(true)))

		rwl.Lock()
		defer rwl.Unlock()
		dbs[name] = db
		return db, nil
	})

	return v.(*gorm.DB), err
}


type OpentracingPlugin struct {
	opt *options
}

func (op OpentracingPlugin) Name() string {
	return "otel"
}

func (op OpentracingPlugin) Initialize(db *gorm.DB) (err error) {
	e := myError{errs: make([]string, 0, 12)}

	// create
	err = db.Callback().Create().Before("gorm:create").Register(_stageBeforeCreate.Name(), op.beforeCreate)
	e.add(_stageBeforeCreate, err)
	err = db.Callback().Create().After("gorm:create").Register(_stageAfterCreate.Name(), op.after)
	e.add(_stageAfterCreate, err)

	// update
	err = db.Callback().Update().Before("gorm:update").Register(_stageBeforeUpdate.Name(), op.beforeUpdate)
	e.add(_stageBeforeUpdate, err)
	err = db.Callback().Update().After("gorm:update").Register(_stageAfterUpdate.Name(), op.after)
	e.add(_stageAfterUpdate, err)

	// query
	err = db.Callback().Query().Before("gorm:query").Register(_stageBeforeQuery.Name(), op.beforeQuery)
	e.add(_stageBeforeQuery, err)
	err = db.Callback().Query().After("gorm:query").Register(_stageAfterQuery.Name(), op.after)
	e.add(_stageAfterQuery, err)

	// delete
	err = db.Callback().Delete().Before("gorm:delete").Register(_stageBeforeDelete.Name(), op.beforeDelete)
	e.add(_stageBeforeDelete, err)
	err = db.Callback().Delete().After("gorm:delete").Register(_stageAfterDelete.Name(), op.after)
	e.add(_stageAfterDelete, err)

	// row
	err = db.Callback().Row().Before("gorm:row").Register(_stageBeforeRow.Name(), op.beforeRow)
	e.add(_stageBeforeRow, err)
	err = db.Callback().Row().After("gorm:row").Register(_stageAfterRow.Name(), op.after)
	e.add(_stageAfterRow, err)

	// raw
	err = db.Callback().Raw().Before("gorm:raw").Register(_stageBeforeRaw.Name(), op.beforeRaw)
	e.add(_stageBeforeRaw, err)
	err = db.Callback().Raw().After("gorm:raw").Register(_stageAfterRaw.Name(), op.after)
	e.add(_stageAfterRaw, err)

	return e.toError()
}

func New(opts ...ApplyOption) gorm.Plugin {
	dst := defaultOption()

	for _, apply := range opts {
		apply(dst)
	}

	return OpentracingPlugin{opt: dst}
}
