const express = require('express');
const fs = require('fs'); 

const app = express();

app.set('view engine', 'ejs');

app.use(express.urlencoded({ extended: false }));


// lista de paginas para mostrar en la UI
var items = [];

// objeto de paginas con pagerank
objetoPageRank = {}

// leer data almacenada en el archivo
readData();

app.get('/', (req, res) => {
  console.log('GET :::::');
  
  res.render('index', { items })

});


app.post('/item/add', (req, res) => {
  console.log('POST :::::');
  
  // Buscando la consulta
  items = []
  items = searchPage(req.body.name);
  
  res.redirect('/')
});

const port = 3000;

app.listen(port, () => console.log('Servidor corriendo en puerto 3000...'));


function readData(){
  console.log('Leendo la data.txt');

  try {
    fs.readFileSync('data.txt', 'utf-8').split(/\r?\n/).forEach(function(line){
            
      let cadena = line.trim().split("\t");      
      for(var i = 0; i < cadena.length; i++){
        if(i === 0){
          objetoPageRank[cadena[i]] = []; 
        } else {
          let pagerank = cadena[i].split(":");
          objetoPageRank[cadena[0]].push(
            {
              title: pagerank[0],
              pagerank: parseInt(pagerank[1],10) 
            }
          );
        } 

      }      
      
    })
    
    console.log('Objeto page rank  .... ', objetoPageRank)
    
  } catch (err) {
    console.error(err)
  }
}

function searchPage(title){
  let items = []
  // buscar el objeto de busqueda.
  if(objetoPageRank[title]){
    let obj = objetoPageRank[title] 
    
    // ordenar los elementos en base al pagerank    
    obj.sort(function(a,b){return b.pagerank - a.pagerank;});

    for (let i = 0; i < obj.length; i++) {
      let elemento = obj[i];
      items.push(elemento.title + ' - ' + elemento.pagerank);
    }        
  }
  return items;
}