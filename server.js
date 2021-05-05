const express = require('express')
const app = express()

const fs = require('fs')
const fastCsv = require('fast-csv')
const moment = require('moment')
const { Readable } = require('stream');
const { Workbook } = require('xlsx-rw');

const SQL = require('mssql')
const mongoose = require('mongoose')
const Schema = mongoose.Schema;

const MongoModel = mongoose.model('collection1', new Schema({}, { strict: false }));
const MongoVerificacaoModel = mongoose.model('collection2', new Schema({}, { strict: false }));

app.use(express.json())

//exportando csv por stream com mongo
app.get('/csv-mongo', async (req, res) => {
  try {

    // caso queira verificar se há exportacao sendo feita
    // const proc = await MongoVerificacaoModel.findOne({ processando: true })
    // if (proc) {
    //     return res.status(400).json({ 
    //         message: 'Há arquivo sendo processando'
    //     })
    // }

    const { date_range } = req.query

    let pipeline = []

    //exporta baseado na data range, senao dia atual
    if (date_range) {
      pipeline = [{
        '$match': {
          'created_at': {
            $gte: moment(date_range.gte, 'X').startOf('day').toISOString(),
            $lte: moment(date_range.lte, 'X').endOf('day').toISOString()
          }
        }
      },
      ...pipeline
      ]
    } else {
      pipeline = [{
        '$match': {
          'created_at': {
            $gte: moment('2021-04-29').startOf('day').toISOString(),
            $lte: moment('2021-04-29').endOf('day').toISOString()
          }
        }
      },
      ...pipeline
      ]
    }


    const cursor = MongoModel.aggregate(pipeline).cursor().exec()

    const timestamp = new Date().getTime()
    const tempoInicio = new Date().getTime()

    //criando um log no database - é opcional
    const processamento = await MongoVerificacaoModel.create({
      processando: true,
      data: moment().toISOString(),
      arquivo: `arquivo-${timestamp}.csv`,
      usuario: '',
      tempo: 0
    })

    let contador = 0
    const transformer = (row) => {
      contador++
      // mapeando os dados
      return {
        "id": row.id,
        "name": row.name,
        "email": row.email
      };
    }

    // const filename = 'export.csv';
    let csvStream = fastCsv.createWriteStream({ headers: true, delimiter: ';' }).transform(transformer)
    let writer = fs.createWriteStream(`./arquivos/csv/umano-${timestamp}.csv`, { encoding: 'utf-8' }).on('close', async () => {
      // "on close" é executado quando finalizar a exportacao do arquivo

      const tempoFinal = new Date().getTime()
      const finalizado = (tempoFinal - tempoInicio) / 1000

      // update no log gerado acima
      const processamentoFinal = await ProcessamentoModel.updateOne({ 
        _id: processamento._id 
      }, {
        processando: false, 
        tempo: finalizado, 
        registros: contador
      })
      console.log('close...')
    });

    
    cursor.pipe(csvStream).pipe(writer);

    res.json({ ok: 'seu arquivo esta sendo processado!' })
  } catch (error) {
    return res.status(400).send({ error: true, message: error.message, location: 'exportacao csv' })
  }
})

//exportando csv por stream com sql
app.get('/csv-sql', async (req, res) => {
  try {

    // caso queira verificar se há exportacao sendo feita
    // const proc = await MongoVerificacaoModel.findOne({ processando: true })
    // if (proc) {
    //     return res.status(400).json({ 
    //         message: 'Há arquivo sendo processando'
    //     })
    // }

    // pegando parametros na query caso queira usar na consulta do db
    const { date_range } = req.query

    const request = new SQL.Request();

    request.stream = true;
    request.query(`SELECT top 1000 * FROM vw_transacoes`);

    const timestamp = new Date().getTime()
    const tempoInicio = new Date().getTime()

    // insere em um doc auxiliar no mongo que há uma exportação em andamento
    // é um "log" - pode usar com o SQL tambem...
    const processamento = await MongoVerificacaoModel.create({
      processando: true,
      data: moment().toISOString(),
      arquivo: `arquivo-${timestamp}.csv`,
      usuario: '',
      tempo: 0
    })

    let contador = 0
    const transformer = (row) => {
      contador++
      // mapeando os dados
      return {
        "id": row.id,
        "name": row.name,
        "email": row.email
      };
    }

    let csvStream = fastCsv.createWriteStream({ headers: true, delimiter: ';' }).transform(transformer)
    let writer = fs.createWriteStream(`./arquivos/csv/umano-${timestamp}.csv`, { encoding: 'utf-8' }).on('close', async () => {
      // "on close" é executado quando finalizar a exportacao do arquivo

      const tempoFinal = new Date().getTime()
      const finalizado = (tempoFinal - tempoInicio) / 1000

      //atualizando arquivo de log criado no mongo - pode usar SQL
      const processamentoFinal = await MongoVerificacaoModel.updateOne({ 
        _id: processamento._id 
      }, {
        processando: false, 
        tempo: finalizado, 
        registros: contador
      })
      console.log('close...')
    });

    request.pipe(csvStream).pipe(writer);

    res.json({ ok: 'seu arquivo esta sendo processado!' })
  } catch (error) {
    return res.status(400).send({ error: true, message: error.message, location: 'exportacao csv' })
  }
})

//exportando csv e xlsx por stream com sql
app.get('/csv-sql-xlsx', async (req, res) => {
  try {

    // caso queira verificar se há exportacao sendo feita
    // const proc = await ProcessamentoModel.findOne({ processando: true })
    // if (proc) {
    //     return res.status(400).json({ 
    //         message: 'Há arquivo sendo processando'
    //     })
    // }

    const { date_range } = req.query

    const request = new SQL.Request();

    request.stream = true;
    request.query(`SELECT top 1000 * FROM vw_transacoes`);

    const timestamp = new Date().getTime()
    const tempoInicio = new Date().getTime()

    // insere em um doc auxiliar no mongo que há uma exportação em andamento
    // é um "log"
    const processamento = await MongoVerificacaoModel.create({
      processando: true,
      data: moment().toISOString(),
      arquivo: `arquivo-${timestamp}.csv`,
      usuario: '',
      tempo: 0
    })

    let contador = 0
    const transformer = (row) => {
      contador++
      // mapeando os dados
      return {
        "id": row.id,
        "name": row.name,
        "email": row.email
      };
    }

    let csvStream = fastCsv.createWriteStream({ headers: true, delimiter: ';' }).transform(transformer)
    let writer = fs.createWriteStream(`./arquivos/csv/umano-${timestamp}.csv`, { encoding: 'utf-8' }).on('close', async () => {
      // "on close" é executado quando finalizar a exportacao do arquivo

      const tempoFinal = new Date().getTime()
      const finalizado = (tempoFinal - tempoInicio) / 1000

      //atualizando arquivo de log criado no mongo
      const processamentoFinal = await MongoVerificacaoModel.updateOne({ 
        _id: processamento._id 
      }, {
        processando: false, 
        tempo: finalizado, 
        registros: contador
      })
      console.log('close...')
    });

    //vou gerar xlsx aqui to nem ai
    //gerando um arquivo xlsx simultaneamente com o csv
    const w = new Workbook()
    let seIssoDerCerto = []
    csvStream.on('data', (data) => {
      // console.log(Buffer.from(data))
      const stream = Readable.from(data.toString());
      const linha = stream.read().toString().split(';')
      //converte cada linha STREAM do csv e insere como array
      //no array "seIssoDerCerto"
      seIssoDerCerto.push(linha)

    })
    csvStream.on('end', (data) => {
      // quando finaliza o ".on" cai no end pra finalizarmos o arquivo
      //primeiro array é o cabecalho
      // o restante sao as linhas com o conteudo do xlsx
      //conteudo é o array seIssoDerCerto
      const arrOfArrs = [
        [
          "id", "name", "email",
        ],
        ...seIssoDerCerto,
      ];

      const wb = w.writeData('exportacao', arrOfArrs);
      const excelLocation = './arquivos/xlsx/umano.xlsx';
      wb.save(excelLocation);
    })
    request.pipe(csvStream).pipe(writer);

    res.json({ ok: 'seu arquivo esta sendo processado!' })
  } catch (error) {
    return res.status(400).send({ error: true, message: error.message, location: 'exportacao csv' })
  }
})

//funcao main
function main() {
  const lis = app.listen({ port: 3333 });
  lis.on("listening", async () => {
    // conecta no mongo
    mongoose.connect('STRING CONEXAO MONGO', {
      promiseLibrary: Promise
    })

    //conecta no SQL
    await SQL.connect('STRING CONEXAO SQL')

    console.info(`Listening on port 3333`);
    console.info("Banco de dados conectado");
  });
}

process.on("unhandledRejection", (err) => {
  if (err) {
    console.error(err);
  }
  process.exit(1);
});


main();