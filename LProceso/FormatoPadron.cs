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
    public class FormatoPadron : IProceso
    {
        #region Objects

        private SqlConnection bdConnection;
        private SqlTransaction spTransaction;
        private const String sp_ImportPadron = "UsuarioImportPadron";
        private const String sp_DeactivateAccount = "UsuarioDeactivateAccount";
        private const String sp_TransferData = "UsuarioTransferData";
        private const String IdTipoInscripcionPromocion = "P";
        private const String IdMovimientoBaja = "B";
        private const String IdMovimientoCambio = "C";
        private const String NoValue = "N";
        private const String SiValue = "S";
        private bool changedAccount = false;

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
            p_smResult.BllReset("FormatoPadron", "Init");

            try
            {
                this.bdConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["InscripcionesCursos"].ConnectionString);
                this.bdConnection.Open();
                this.spTransaction = bdConnection.BeginTransaction("TransactionPadron");
            }
            catch (Exception l_expData)
            {
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
            p_smResult.BllReset("FormatoPadron", "Process");

            try
            {
                #region Validations
                double numCheck;

                //VALIDA DNI
                if (p_astrData[0].Trim().Length == 0)
                {
                    p_smResult.BllError("El DNI debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[0], out numCheck))
                    {
                        p_smResult.BllError(String.Format("El DNI {0} debe ser del tipo int.", p_astrData[0]));
                        return;
                    }
                }

                if (p_astrData[19].Trim().Length != 0)
                {
                    if (!double.TryParse(p_astrData[16], out numCheck))
                    {
                        p_smResult.BllError(String.Format("El DNI {0} debe ser del tipo int.", p_astrData[0]));
                        return;
                    }
                }

                //VALIDA APELLIDONOMBRE
                if (p_astrData[1].Trim().Length == 0)
                {
                    p_smResult.BllError("El ApellidoNombre debe contener un valor.");
                    return;
                }

                //VALIDA ID_SEDE
                if (p_astrData[2].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Sede debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[2], out numCheck))
                    {
                        p_smResult.BllError("El Id de Sede debe ser del tipo int.");
                        return;
                    }
                }

                //VALIDA ID_ESTADO
                if (p_astrData[3].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Estado debe contener un valor.");
                    return;
                }

                //VALIDA ID_CARRERA
                if (p_astrData[4].Trim().Length == 0)
                {
                    p_smResult.BllError("El Id de Carrera debe contener un valor.");
                    return;
                }
                else
                {
                    if (!double.TryParse(p_astrData[4], out numCheck))
                    {
                        p_smResult.BllError("El Id de Carrera debe ser del tipo int.");
                        return;
                    }
                }

                #endregion

                #region Delete Student && Deactivate account

                if (p_astrData[18].Trim() != string.Empty)
                {
                    switch(p_astrData[18].Trim().ToUpper())
                    {
                        case IdMovimientoBaja:
                            DeactivateAccount(Convert.ToInt32(p_astrData[0]));
                            changedAccount = true;
                            break;
                        case IdMovimientoCambio:
                            if (p_astrData[19].Trim() != string.Empty)
                                TransferData(Convert.ToInt32(p_astrData[0].Trim()), Convert.ToInt32(p_astrData[19].Trim()));
                            changedAccount = true;
                            break;
                        default:
                            break;
                    }
                }

                #endregion

                if (!changedAccount)
                {
                    using (SqlCommand cmd = new SqlCommand(sp_ImportPadron, this.bdConnection))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.AddWithValue("@DNI", Convert.ToInt32(p_astrData[0].Trim()));
                        cmd.Parameters.AddWithValue("@ApellidoNombre", p_astrData[1].Trim().Replace('�', 'Ñ'));
                        cmd.Parameters.AddWithValue("@IdSede", Convert.ToInt32(p_astrData[2]));
                        cmd.Parameters.AddWithValue("@IdEstado", p_astrData[3].Trim());
                        cmd.Parameters.AddWithValue("@IdCarrera", Convert.ToInt32(p_astrData[4].Trim()));
                        cmd.Parameters.AddWithValue("@CuatrimestreAnioIngreso", ((Object)p_astrData[5].Trim() ?? DBNull.Value));
                        cmd.Parameters.AddWithValue("@CuatrimestreAnioReincorporacion", ((Object)p_astrData[6].Trim() ?? DBNull.Value));
                        cmd.Parameters.AddWithValue("@IdCargo", 2);

                        if (p_astrData[8].Trim().Length > 0)
                        {
                            cmd.Parameters.AddWithValue("@LimitacionRelevada", true);
                            cmd.Parameters.AddWithValue("@Limitacion", p_astrData[8].Trim());
                            cmd.Parameters.AddWithValue("@LimitacionVision", p_astrData[9].Trim());
                            cmd.Parameters.AddWithValue("@Lentes", p_astrData[10].Trim());
                            cmd.Parameters.AddWithValue("@LimitacionAudicion", p_astrData[11].Trim());
                            cmd.Parameters.AddWithValue("@Audifonos", p_astrData[12].Trim());
                            cmd.Parameters.AddWithValue("@LimitacionMotriz", p_astrData[13].Trim());                            
                            cmd.Parameters.AddWithValue("@LimitacionAgarre", p_astrData[14].Trim());
                            cmd.Parameters.AddWithValue("@LimitacionHabla", p_astrData[15].Trim());
                            cmd.Parameters.AddWithValue("@Dislexia", p_astrData[16].Trim());
                            cmd.Parameters.AddWithValue("@LimitacionOtra", p_astrData[17].Trim());
                        }
                        else
                        {
                            cmd.Parameters.AddWithValue("@LimitacionRelevada", false);
                            cmd.Parameters.AddWithValue("@Limitacion", DBNull.Value);
                            cmd.Parameters.AddWithValue("@LimitacionVision", DBNull.Value);
                            cmd.Parameters.AddWithValue("@Lentes", DBNull.Value);
                            cmd.Parameters.AddWithValue("@LimitacionMotriz", DBNull.Value);
                            cmd.Parameters.AddWithValue("@LimitacionAudicion", DBNull.Value);
                            cmd.Parameters.AddWithValue("@Audifonos", DBNull.Value);
                            cmd.Parameters.AddWithValue("@LimitacionAgarre", DBNull.Value);
                            cmd.Parameters.AddWithValue("@LimitacionHabla", DBNull.Value);
                            cmd.Parameters.AddWithValue("@Dislexia", DBNull.Value);
                            cmd.Parameters.AddWithValue("@LimitacionOtra", DBNull.Value); 
                        }

                        cmd.Transaction = this.spTransaction;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception l_expData)
            {
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
        /// Transfiere la información del DNI viejo al nuevo DNI del alumno, actualiza Padron de calificaciones e Inscripciones, para luego eliminar
        /// el DNI viejo
        /// </summary>
        /// <param name="dniOld">DNI origen de la información a transferir</param>
        /// <param name="dniNew">DNI a transferir la información</param>
        private void TransferData(int dniOld, int dniNew)
        {
            using (SqlCommand cmd = new SqlCommand(sp_TransferData, this.bdConnection))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@DNIOld", dniOld);
                cmd.Parameters.AddWithValue("@DNINew", dniNew);

                cmd.Transaction = this.spTransaction;
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Desactiva la cuenta del DNI seteado
        /// </summary>
        /// <param name="dni"></param>
        private void DeactivateAccount(int dni)
        {
            using (SqlCommand cmd = new SqlCommand(sp_DeactivateAccount, this.bdConnection))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@DNI", dni);

                cmd.Transaction = this.spTransaction;
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Finaliza el proceso de los registros de un archivo de Formato Uno
        /// </summary>
        /// <param name="p_smResult">Estado final de la operacion</param>
        public void End(ref StatMsg p_smResult)
        {
            // No hay errores aun
            p_smResult.BllReset("FormatoPadron", "End");

            try
            {
                if (spTransaction != null)
                    spTransaction.Commit();

                if (this.bdConnection != null)
                {
                    this.bdConnection.Close();
                    this.bdConnection.Dispose();
                }
            }
            catch (Exception l_expData)
            {
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

                if (this.bdConnection != null)
                {
                    this.bdConnection.Close();
                    this.bdConnection.Dispose();
                }
            }
            catch (Exception)
            {
            }
        }
    }
}
