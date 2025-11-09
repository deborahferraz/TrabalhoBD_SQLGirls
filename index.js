const { Client } = require('pg');
const fs = require('fs');

// 1. Configurações de Conexão com PostgreSQL
// Para fins de teste no ambiente, usaremos um banco de dados local.
// O usuário deverá adaptar estas credenciais para o seu ambiente PgAdmin/PostgreSQL.
const client = new Client({
    host: 'localhost',
    port: 5432,
    database: 'Trabalho_BD', // Usaremos o banco de dados padrão 'postgres' para criar a tabela
    user: 'postgres',
    password: 'traficante123?' // Senha padrão para o PostgreSQL em muitos ambientes de desenvolvimento
});

// 2. Função principal para descobrir as dependências funcionais
async function discoverFunctionalDependencies() {
    try {
        // Conectar ao banco de dados
        await client.connect();
        console.log("Conectado ao banco de dados PostgreSQL.");

        // Configurar o banco de dados (criar tabela e inserir dados)
        await setupDatabase();

        console.log("\nIniciando a descoberta de Dependências Funcionais...");

        // A tabela é 'carros'
        const tableName = 'carros';
        
        // Obter a lista de colunas
        const columns = await getColumns(tableName);
        if (columns.length === 0) {
            console.log("Nenhuma coluna encontrada na tabela.");
            await client.end();
            return;
        }
        console.log("Colunas:", columns);

        const validDependencies = [];

        // Gerar todas as combinações possíveis de atributos para o lado esquerdo (LE)
        // 1, 2 ou 3 atributos
        for (let numAttributes = 1; numAttributes <= 3; numAttributes++) {
            const combinations = generateCombinations(columns, numAttributes);
            
            for (const le of combinations) {
                // Para cada combinação do lado esquerdo (LE), testar cada atributo do lado direito (LD)
                for (const ld of columns) {
                    // O lado direito não pode ser um atributo do lado esquerdo
                    if (!le.includes(ld)) {
                        const isFunctionalDependency = await checkFunctionalDependency(tableName, le, ld);
                        
                        if (isFunctionalDependency) {
                            validDependencies.push({ le: le.join(', '), ld: ld });
                        }
                    }
                }
            }
        }

        console.log("\n==================================================");
        console.log("Descoberta de Dependências Funcionais Concluída!");
        console.log("Tabela: " + tableName);
        console.log("Total de Dependências Válidas Encontradas: " + validDependencies.length);
        console.log("==================================================");
        
        validDependencies.forEach(dep => {
            console.log(`[${dep.le}] -> [${dep.ld}]`);
        });

    } catch (err) {
        console.error("Erro durante a execução:", err.message);
    } finally {
        // Fechar a conexão com o banco de dados
        await client.end();
        console.log("\nDesconectado do banco de dados.");
    }
}

// 3. Configurar o banco de dados (criar tabela e inserir dados)
async function setupDatabase() {
const sql = fs.readFileSync('./data/database.sql', 'utf8');

    // Adicionar comando para dropar a tabela se ela existir, para garantir um estado limpo
    const dropTableSql = 'DROP TABLE IF EXISTS carros;';
    
    try {
        await client.query(dropTableSql);
        await client.query(sql);
        console.log("Tabela 'carros' configurada e populada.");
    } catch (err) {
        console.error("Erro ao configurar o banco de dados:", err.message);
        throw err; // Propagar o erro para interromper a execução
    }
}

// Função auxiliar para obter a lista de colunas da tabela
async function getColumns(tableName) {
    const sql = `
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = $1 
        ORDER BY ordinal_position;
    `;
    
    try {
        const result = await client.query(sql, [tableName]);
        return result.rows.map(row => row.column_name);
    } catch (err) {
        console.error("Erro ao obter colunas:", err.message);
        return [];
    }
}

// Função auxiliar para gerar combinações (subconjuntos) de um array
function generateCombinations(array, size) {
    const results = [];
    function backtrack(start, currentCombination) {
        if (currentCombination.length === size) {
            results.push([...currentCombination]);
            return;
        }
        for (let i = start; i < array.length; i++) {
            currentCombination.push(array[i]);
            backtrack(i + 1, currentCombination);
            currentCombination.pop();
        }
    }
    backtrack(0, []);
    return results;
}

// 4. A Lógica da Query de Verificação (X -> Y)
// Se para cada grupo de valores distintos de X, houver apenas 1 valor distinto de Y, a dependência é válida.
// Se houver algum grupo de X com mais de 1 valor distinto de Y, a dependência é violada.
async function checkFunctionalDependency(tableName, le, ld) {
    const leString = le.join(', ');
    
    // A query busca grupos de LE (lado esquerdo) que possuem mais de um valor distinto de LD (lado direito).
    // Se a query retornar 0 linhas, significa que não houve violação, e a dependência é válida.
    const sql = `
        SELECT ${leString}, COUNT(DISTINCT ${ld}) as distinct_ld_count
        FROM ${tableName}
        GROUP BY ${leString}
        HAVING COUNT(DISTINCT ${ld}) > 1
    `;

    try {
        const result = await client.query(sql);
        // Se result.rows.length for 0, a dependência é válida (não há violações)
        return result.rows.length === 0;
    } catch (err) {
        console.error(`Erro ao verificar dependência [${leString}] -> [${ld}]:`, err.message);
        // Em caso de erro na query, assumimos que a dependência não pode ser validada (ou é inválida)
        return false;
    }
}

// Executar a função principal
discoverFunctionalDependencies();
