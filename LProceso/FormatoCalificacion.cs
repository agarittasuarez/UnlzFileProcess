using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using TNGS.NetRoutines;
using Unlz.Interfaces;
using System.Data.SqlClient;
using System.Data;
using System.Data.SqlTypes;
using System.Configuration;

namespace Unlz.FileProcess
{
    public class FormatoCalificacion : IProceso
    {
        #region Objects

        private SqlConnection bdConnection= null;
        private SqlTransaction spTransaction= null;
        private const String sp_ImportNotas = "AnaliticoImportNotas";

        #endregion

        /// <summary>
        /// Devuelve el formato del registro
        /// </summary>
        public ArrayList GetFormat()
        {
            // No hay formato
            return null;
        }

        /// <summary>
        /// Devuelve el delimitador de campos
        /// </summary>
        public string GetDelimiter()
        {
            // Devolvemos el delimitador
            return ";";
        }

        /// <summary>
        /// Incia el proceso de los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_strFileName">Nombre del archivo a procesar</param>
        /// <param name="p_strExtraData">Datos extras asociados</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void Init(string p_strFileName, string p_strExtraData, ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoCalificacion", "Init");

            try
            {                
                this.bdConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["InscripcionesCursos"].ConnectionString);
                this.bdConnection.Open();
                this.spTransaction = bdConnection.BeginTransaction("TransactionCalificaciones");
            }
            catch (Exception l_expData) {
                // La captura de un error se reporta siempre como
                // grave y produce la cancelación del proceso.
                p_smResult.BllError(l_expData.ToString());
            }
            finally
            {
                p_smResult.BllPop();
            }
        }

        /// <summary>
        /// Procesa los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_iNroRec">Numero de registro</param>
        /// <param name="p_astrData">Datos del registro a procesar</param>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void Process(int p_iNroRec, string[] p_astrData, ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoCalificacion", "Process");

            try
            {
                #region Validations
                double numCheck;
                DateTime dateCheck;

                //VALIDA IDMATERIA
                if (p_astrData[2].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Materia debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[2], out numCheck))
                    {
                        p_smResult.BllError("El Id de Materia debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA TURNO DE INSCRIPCION
                if (p_astrData[1].Trim().Length == 0)
                {
                    p_smResult.BllError("El Turno de Inscripcion debe contener un valor.");
                    return;
                }
                else
                {
                    if (!DateTime.TryParse(p_astrData[1], out dateCheck))
                    {
                        p_smResult.BllError("El Turno de Inscripcion debe ser del tipo DateTime.");
                        return;
                    }
                }

                //VALIDA TIPO INSCRIPCION
                if (p_astrData[0].Trim().Length == 0)
                {
                    p_smResult.BllError("El Tipo de Inscripcion debe contener un valor.");
                    return;
                }

                //VALIDA DNI
                if (p_astrData[4].Trim().Length == 0)
                {
                    p_smResult.BllError("El DNI debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[4], out numCheck))
                    {
                        p_smResult.BllError("El DNI debe ser del tipo int.");
                        return;
                    }
                }
                #region OldValidate
                ////VALIDA CATEDRA COMISION
                //if (p_astrData[3].Trim().Length == 0)
                //{
                //    p_smResult.BllError("La Catedra Comision debe contener un valor.");
                //    return;
                //}
                //VALIDA PLAN
                //if (p_astrData[5].Trim().Length == 0)
                //{
                //    p_smResult.BllError("El Plan debe contener un valor.");
                //    return;
                //}
                //else
                //{
                //    if (!double.TryParse(p_astrData[5], out numCheck))
                //    {
                //        p_smResult.BllError("El Plan debe ser del tipo int.");
                //        return;
                //    }
                //}

                ////VALIDA FECHA
                //if (p_astrData[6].Trim().Length == 0)
                //{
                //    p_smResult.BllError("La Fecha debe contener un valor.");
                //    return;
                //}
                //else
                //{
                //    if (!DateTime.TryParse(p_astrData[6], out dateCheck))
                //    {
                //        p_smResult.BllError("La Fecha debe ser del tipo DateTime.");
                //        return;
                //    }
                //}

                ////VALIDA NOTA
                //if (p_astrData[7].Trim().Length == 0)
                //{
                //    p_smResult.BllError("La Nota debe contener un valor.");
                //    return;
                //}
                //else
                //{
                //    if (!double.TryParse(p_astrData[7], out numCheck))
                //    {
                //        p_smResult.BllError("La Nota debe ser del tipo float.");
                //        return;
                //    }
                //}

                ////VALIDA LIBRO
                //if (p_astrData[8].Trim().Length == 0)
                //{
                //    p_smResult.BllError("El Libro debe contener un valor.");
                //    return;
                //}

                ////VALIDA TOMO
                //if (p_astrData[9].Trim().Length == 0)
                //{
                //    p_smResult.BllError("El Tomo debe contener un valor.");
                //    return;
                //}

                ////VALIDA FOLIO
                //if (p_astrData[10].Trim().Length == 0)
                //{
                //    p_smResult.BllError("El Folio debe contener un valor.");
                //    return;
                //}

                ////VALIDA RESOLUCION
                //if (p_astrData[13].Trim().Length == 0)
                //{
                //    p_smResult.BllError("La resolucion debe contener un valor.");
                //    return;
                //}

                #endregion

                #endregion

                using (SqlCommand cmd = new SqlCommand(sp_ImportNotas, this.bdConnection))
                {

                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@IdTipoInscripcion", SqlDbType.Char).Value = p_astrData[0];
                    cmd.Parameters.Add("@TurnoInscripcion", SqlDbType.Date).Value = p_astrData[1].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[1]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@IdMateria", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[2]);
                    cmd.Parameters.Add("@CatedraComision", SqlDbType.VarChar).Value = p_astrData[3];
                    cmd.Parameters.Add("@DNI", SqlDbType.Int).Value = Convert.ToInt32(p_astrData[4]);
                    cmd.Parameters.Add("@Plan", SqlDbType.Int).Value = p_astrData[5].Trim().Length > 0 ? Convert.ToInt32(p_astrData[5]) : -1;
                    cmd.Parameters.Add("@Fecha", SqlDbType.Date).Value = p_astrData[6].Trim().Length > 0 ? Convert.ToDateTime(p_astrData[6]) : (DateTime)SqlDateTime.Null;
                    cmd.Parameters.Add("@Nota", SqlDbType.Float).Value = p_astrData[7].Trim().Length > 0 ? Convert.ToDouble(p_astrData[7]) : -1;
                    cmd.Parameters.Add("@Libro", SqlDbType.VarChar).Value = p_astrData[8];
                    cmd.Parameters.Add("@Tomo", SqlDbType.VarChar).Value = p_astrData[9];
                    cmd.Parameters.Add("@Folio", SqlDbType.VarChar).Value = p_astrData[10];
                    cmd.Parameters.Add("@SubFolio", SqlDbType.VarChar).Value = p_astrData[11];
                    cmd.Parameters.Add("@Resolucion", SqlDbType.VarChar).Value = p_astrData[12];
                    cmd.Parameters.Add("@CodigoMovimiento", SqlDbType.Char).Value = p_astrData[13];

                    cmd.Transaction = this.spTransaction;
                    cmd.ExecuteNonQuery();
                }
                
            }
            catch (Exception l_expData) {
                // La captura de un error se reporta siempre como
                // grave y produce la cancelación del proceso.
                p_smResult.BllError(l_expData.ToString());
            }
            finally
            {
                p_smResult.BllPop();
            }
        }

        /// <summary>
        /// Finaliza el proceso de los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void End(ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoCalificacion", "End");

            try
            {
                if (spTransaction != null)
                    spTransaction.Commit();

                if (this.bdConnection != null) {
                    this.bdConnection.Close();
                    this.bdConnection.Dispose();
                }
            }
            catch (Exception l_expData) {
                // La captura de un error se reporta siempre como
                // grave y produce la cancelación del proceso.
                p_smResult.BllError(l_expData.ToString());
            }
            finally
            {
                p_smResult.BllPop();
            }
        }

        /// <summary>
        /// Indica que se aborto el proceso por problemas en la capa de proceso
        /// </summary>
        /// <param name="p_smResult">Estado de error de la operacion</param>
        public void Abort(StatMsg p_smResult)
        {
            try
            {
                // Rollback de la transaccion
                if (spTransaction != null)
                    spTransaction.Rollback();

                if (this.bdConnection != null) {
                    this.bdConnection.Close();
                    this.bdConnection.Dispose();
                }
            }
            catch (Exception) {
            }
        }
    }
}
